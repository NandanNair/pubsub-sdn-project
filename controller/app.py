
"""
Ryu OpenFlow 1.3 Controller for Pub-Sub SDN Project.

Features:
- MAC learning
- TCP traffic classification:
    Port 5001 -> HIGH priority 100
    Port 5002 -> NORMAL priority 50
- Static and dynamic routing
- Class-aware path selection in dynamic mode
- HIGH: direct s1-s3 path
- NORMAL: s1-s2-s3 path
- BFS shortest-path fallback
- BFS spanning-tree flooding
- Link failure detection and recovery
- Flow-table cleanup
- Periodic flow statistics monitoring
- REST API for subscriptions, modes, statistics and IP blocking
"""

import json

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import (
    CONFIG_DISPATCHER,
    MAIN_DISPATCHER,
    set_ev_cls
)
from ryu.lib import hub
from ryu.lib.packet import (
    ether_types,
    ethernet,
    packet,
    ipv4,
    tcp
)
from ryu.ofproto import ofproto_v1_3
from ryu.topology import event as topo_event
from ryu.topology.api import get_switch, get_link
from ryu.app.wsgi import ControllerBase, WSGIApplication, route


TRAFFIC_CLASSES = {
    5001: ("HIGH", 100),
    5002: ("NORMAL", 50)
}

PRIORITY_L2 = 1
PRIORITY_TABLE_MISS = 0
PRIORITY_BLOCK = 200

MONITOR_INTERVAL = 5

SWITCH_1 = 1
SWITCH_2 = 2
SWITCH_3 = 3


class PubSubController(app_manager.RyuApp):

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    _CONTEXTS = {
        'wsgi': WSGIApplication
    }

    def __init__(self, *args, **kwargs):

        super(PubSubController, self).__init__(*args, **kwargs)

        self.mac_to_port = {}
        self.datapaths = {}

        self.last_counts = {}

        self.class_counts_by_switch = {
            "HIGH": {},
            "NORMAL": {}
        }

        self.class_totals = {
            "HIGH": 0,
            "NORMAL": 0
        }

        self.switches = []

        self.links = {}
        self.reverse_links = {}
        self.adjacency = {}

        self.down_ports = set()

        self.known_links = {}
        self.port_peers = {}
        self.link_status = {}

        self.subscriptions = {}
        self.blocked_ips = set()

        self.mode = "dynamic"
        self.static_paths_cache = {}

        wsgi = kwargs['wsgi']

        wsgi.register(
            PubSubRestController,
            {'pubsub_app': self}
        )

        self.monitor_thread = hub.spawn(self._monitor)

        self.logger.info(
            "PubSub SDN Controller started in %s mode",
            self.mode
        )

    # ---------------------------------------------------------
    # Switch connection and flow installation
    # ---------------------------------------------------------

    @set_ev_cls(
        ofp_event.EventOFPSwitchFeatures,
        CONFIG_DISPATCHER
    )
    def switch_features_handler(self, ev):
        """Registers a switch and installs its initial rules."""

        datapath = ev.msg.datapath
        dpid = datapath.id

        self.down_ports = {
            p for p in self.down_ports
            if p[0] != dpid
        }

        self.mac_to_port.pop(dpid, None)
        self.static_paths_cache.clear()

        self.datapaths[dpid] = datapath

        self.install_table_miss(datapath)
        self.install_block_rules(datapath)

        self.logger.info(
            "Switch %s connected",
            dpid
        )

    def install_table_miss(self, datapath):
        """Sends unmatched packets to the controller."""

        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        match = parser.OFPMatch()

        actions = [
            parser.OFPActionOutput(
                ofproto.OFPP_CONTROLLER,
                ofproto.OFPCML_NO_BUFFER
            )
        ]

        self.add_flow(
            datapath,
            PRIORITY_TABLE_MISS,
            match,
            actions
        )

    def add_flow(
        self,
        datapath,
        priority,
        match,
        actions,
        buffer_id=None
    ):
        """Creates and installs an OpenFlow flow entry."""

        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        instructions = [
            parser.OFPInstructionActions(
                ofproto.OFPIT_APPLY_ACTIONS,
                actions
            )
        ]

        if (
            buffer_id is not None
            and buffer_id != ofproto.OFP_NO_BUFFER
        ):

            mod = parser.OFPFlowMod(
                datapath=datapath,
                buffer_id=buffer_id,
                priority=priority,
                match=match,
                instructions=instructions
            )

        else:

            mod = parser.OFPFlowMod(
                datapath=datapath,
                priority=priority,
                match=match,
                instructions=instructions
            )

        datapath.send_msg(mod)

    # ---------------------------------------------------------
    # IP blocking
    # ---------------------------------------------------------

    def install_block_rules(self, datapath):
        """Installs drop rules for blocked source and destination IPs."""

        parser = datapath.ofproto_parser

        for ip in self.blocked_ips:

            source_match = parser.OFPMatch(
                eth_type=ether_types.ETH_TYPE_IP,
                ipv4_src=ip
            )

            destination_match = parser.OFPMatch(
                eth_type=ether_types.ETH_TYPE_IP,
                ipv4_dst=ip
            )

            self.add_flow(
                datapath,
                PRIORITY_BLOCK,
                source_match,
                []
            )

            self.add_flow(
                datapath,
                PRIORITY_BLOCK,
                destination_match,
                []
            )

    # ---------------------------------------------------------
    # Topology discovery
    # ---------------------------------------------------------

    def refresh_topology(self):
        """Rebuilds the active network topology."""

        switch_list = get_switch(self, None)
        link_list = get_link(self, None)

        self.switches = [
            switch.dp.id
            for switch in switch_list
        ]

        self.links.clear()
        self.reverse_links.clear()
        self.adjacency.clear()

        for switch_id in self.switches:
            self.adjacency[switch_id] = {}

        discovered = set()

        for link in link_list:

            src_dpid = link.src.dpid
            dst_dpid = link.dst.dpid

            src_port = link.src.port_no
            dst_port = link.dst.port_no

            discovered.add(
                (src_dpid, src_port, dst_dpid, dst_port)
            )

            self.port_peers[(src_dpid, src_port)] = (
                dst_dpid,
                dst_port
            )

            self.port_peers[(dst_dpid, dst_port)] = (
                src_dpid,
                src_port
            )

            link_key = tuple(
                sorted((src_dpid, dst_dpid))
            )

            self.known_links[link_key] = {
                "source": min(src_dpid, dst_dpid),
                "destination": max(src_dpid, dst_dpid)
            }

            if (
                (src_dpid, src_port) not in self.down_ports
                and (dst_dpid, dst_port) not in self.down_ports
            ):

                self.link_status[link_key] = "UP"

                self.links[
                    (src_dpid, dst_dpid)
                ] = src_port

                self.reverse_links[
                    (src_dpid, src_port)
                ] = (dst_dpid, dst_port)

                self.adjacency.setdefault(
                    src_dpid,
                    {}
                )

                self.adjacency[src_dpid][dst_dpid] = src_port

            else:

                self.link_status[link_key] = "DOWN"

        for link_key in self.known_links:

            if self.link_status.get(link_key) != "DOWN":

                found = any(
                    tuple(sorted((item[0], item[2]))) == link_key
                    for item in discovered
                )

                if not found:
                    self.link_status[link_key] = "DOWN"

        if self.mode != "static":
            self.static_paths_cache.clear()

        self.logger.info(
            "Topology refreshed: switches=%s, directed links=%d",
            self.switches,
            len(self.links)
        )

    @set_ev_cls(
        [
            topo_event.EventSwitchEnter,
            topo_event.EventSwitchLeave,
            topo_event.EventLinkAdd,
            topo_event.EventLinkDelete
        ],
        MAIN_DISPATCHER
    )
    def topology_change_handler(self, ev):
        """Handles topology changes."""

        self.refresh_topology()

        if self.mode == "static":

            self.logger.info(
                "Static mode: topology recorded without rerouting"
            )

            return

        self.flush_all_switches()

    # ---------------------------------------------------------
    # Link failure and recovery
    # ---------------------------------------------------------

    @set_ev_cls(
        ofp_event.EventOFPPortStatus,
        MAIN_DISPATCHER
    )
    def port_status_handler(self, ev):
        """Handles port-down and port-up events."""

        msg = ev.msg

        datapath = msg.datapath
        ofproto = datapath.ofproto

        dpid = datapath.id
        port_no = msg.desc.port_no

        remote_link = self.port_peers.get(
            (dpid, port_no)
        )

        is_down = bool(
            (msg.desc.state & ofproto.OFPPS_LINK_DOWN)
            or (msg.desc.config & ofproto.OFPPC_PORT_DOWN)
            or (msg.reason == ofproto.OFPPR_DELETE)
        )

        changed = False

        if is_down:

            if (dpid, port_no) not in self.down_ports:

                self.down_ports.add(
                    (dpid, port_no)
                )

                changed = True

            if remote_link is not None:

                remote_dpid, remote_port = remote_link

                if (
                    remote_dpid,
                    remote_port
                ) not in self.down_ports:

                    self.down_ports.add(
                        (remote_dpid, remote_port)
                    )

                    changed = True

        else:

            if (dpid, port_no) in self.down_ports:

                self.down_ports.remove(
                    (dpid, port_no)
                )

                changed = True

            if remote_link is not None:

                remote_dpid, remote_port = remote_link

                if (
                    remote_dpid,
                    remote_port
                ) in self.down_ports:

                    self.down_ports.remove(
                        (remote_dpid, remote_port)
                    )

                    changed = True

        if changed:

            link_key = None

            if remote_link is not None:

                link_key = tuple(
                    sorted((dpid, remote_link[0]))
                )

            if link_key is not None:

                self.link_status[link_key] = (
                    "DOWN" if is_down else "UP"
                )

            self.logger.info(
                "Port status changed: switch=%d port=%d down=%s",
                dpid,
                port_no,
                is_down
            )

            self.refresh_topology()

            if self.mode == "static":

                self.logger.info(
                    "Static mode: automatic rerouting disabled"
                )

                return

            self.static_paths_cache.clear()
            self.flush_all_switches()

    # ---------------------------------------------------------
    # Flow-table cleanup
    # ---------------------------------------------------------

    def flush_all_switches(self):
        """Deletes old flows and reinstalls essential rules."""

        for datapath in list(self.datapaths.values()):

            ofproto = datapath.ofproto
            parser = datapath.ofproto_parser

            delete_message = parser.OFPFlowMod(
                datapath=datapath,
                command=ofproto.OFPFC_DELETE,
                table_id=ofproto.OFPTT_ALL,
                out_port=ofproto.OFPP_ANY,
                out_group=ofproto.OFPG_ANY,
                match=parser.OFPMatch()
            )

            datapath.send_msg(delete_message)

            self.install_table_miss(datapath)
            self.install_block_rules(datapath)

        self.last_counts.clear()

        self.class_counts_by_switch = {
            "HIGH": {},
            "NORMAL": {}
        }

        self.class_totals = {
            "HIGH": 0,
            "NORMAL": 0
        }

        self.logger.info(
            "Flow tables cleaned and essential rules reinstalled"
        )

    # ---------------------------------------------------------
    # Host-port identification
    # ---------------------------------------------------------

    def is_host_port(self, dpid, port_no):
        """Checks whether a port is connected to a host."""

        inter_switch_ports = set(
            self.adjacency.get(dpid, {}).values()
        )

        return (
            port_no not in inter_switch_ports
            and (dpid, port_no) not in self.down_ports
        )

    # ---------------------------------------------------------
    # BFS spanning-tree flooding
    # ---------------------------------------------------------

    def get_spanning_tree_ports(self, dpid):
        """Finds loop-free switch ports using BFS."""

        if not self.switches:
            return set()

        root = min(self.switches)

        tree_edges = set()

        visited = {root}
        queue = [root]

        while queue:

            current = queue.pop(0)

            for neighbor in sorted(
                self.adjacency.get(current, {}).keys()
            ):

                if neighbor not in visited:

                    visited.add(neighbor)

                    tree_edges.add(
                        (current, neighbor)
                    )

                    tree_edges.add(
                        (neighbor, current)
                    )

                    queue.append(neighbor)

        tree_ports = set()

        for neighbor, port in self.adjacency.get(
            dpid, {}
        ).items():

            if (dpid, neighbor) in tree_edges:
                tree_ports.add(port)

        return tree_ports

    # ---------------------------------------------------------
    # BFS shortest path
    # ---------------------------------------------------------

    def get_shortest_path(self, src_dpid, dst_dpid):
        """Finds the shortest available path using BFS."""

        if src_dpid == dst_dpid:
            return [src_dpid]

        queue = [[src_dpid]]
        visited = {src_dpid}

        while queue:

            path = queue.pop(0)
            current = path[-1]

            if current == dst_dpid:
                return path

            for neighbor in self.adjacency.get(
                current, {}
            ):

                if neighbor not in visited:

                    visited.add(neighbor)

                    new_path = list(path)
                    new_path.append(neighbor)

                    queue.append(new_path)

        return []

    # ---------------------------------------------------------
    # Static routing
    # ---------------------------------------------------------

    def build_static_path_cache(self):
        """Stores shortest paths for static-mode forwarding."""

        self.static_paths_cache.clear()

        for src_dpid in self.switches:

            for dst_dpid in self.switches:

                if src_dpid == dst_dpid:
                    continue

                path = self.get_shortest_path(
                    src_dpid,
                    dst_dpid
                )

                self.static_paths_cache[
                    (src_dpid, dst_dpid)
                ] = path

    # ---------------------------------------------------------
    # Class-aware path selection
    # ---------------------------------------------------------

    def get_class_path(
        self,
        src_dpid,
        dst_dpid,
        traffic_class
    ):
        """Selects paths for HIGH and NORMAL traffic."""

        if self.mode != "dynamic":

            return self.get_shortest_path(
                src_dpid,
                dst_dpid
            )

        if traffic_class == "HIGH":

            if (
                src_dpid == SWITCH_1
                and dst_dpid == SWITCH_3
                and SWITCH_3 in self.adjacency.get(
                    SWITCH_1, {}
                )
            ):

                return [
                    SWITCH_1,
                    SWITCH_3
                ]

            if (
                src_dpid == SWITCH_3
                and dst_dpid == SWITCH_1
                and SWITCH_1 in self.adjacency.get(
                    SWITCH_3, {}
                )
            ):

                return [
                    SWITCH_3,
                    SWITCH_1
                ]

        if traffic_class == "NORMAL":

            if (
                src_dpid == SWITCH_1
                and dst_dpid == SWITCH_3
                and SWITCH_2 in self.adjacency.get(
                    SWITCH_1, {}
                )
                and SWITCH_3 in self.adjacency.get(
                    SWITCH_2, {}
                )
            ):

                return [
                    SWITCH_1,
                    SWITCH_2,
                    SWITCH_3
                ]

            if (
                src_dpid == SWITCH_3
                and dst_dpid == SWITCH_1
                and SWITCH_2 in self.adjacency.get(
                    SWITCH_3, {}
                )
                and SWITCH_1 in self.adjacency.get(
                    SWITCH_2, {}
                )
            ):

                return [
                    SWITCH_3,
                    SWITCH_2,
                    SWITCH_1
                ]

        return self.get_shortest_path(
            src_dpid,
            dst_dpid
        )

    # ---------------------------------------------------------
    # Select path according to current mode
    # ---------------------------------------------------------

    def get_selected_path(
        self,
        src_dpid,
        dst_dpid,
        traffic_class=None
    ):
        """Returns the path selected for the current traffic."""

        if src_dpid == dst_dpid:
            return [src_dpid]

        if self.mode == "static":

            cache_key = (
                src_dpid,
                dst_dpid
            )

            if cache_key not in self.static_paths_cache:

                self.static_paths_cache[cache_key] = (
                    self.get_shortest_path(
                        src_dpid,
                        dst_dpid
                    )
                )

            return self.static_paths_cache[cache_key]

        return self.get_class_path(
            src_dpid,
            dst_dpid,
            traffic_class
        )

    def get_out_port(
        self,
        src_dpid,
        dst_dpid,
        dst_mac,
        in_port,
        traffic_class=None
    ):
        """Returns the next-hop port for the selected path."""

        if src_dpid == dst_dpid:

            return self.mac_to_port.get(
                src_dpid, {}
            ).get(dst_mac)

        path = self.get_selected_path(
            src_dpid,
            dst_dpid,
            traffic_class
        )

        if len(path) > 1:

            next_hop = path[1]

            return self.adjacency.get(
                src_dpid, {}
            ).get(next_hop)

        return None

    # ---------------------------------------------------------
    # Install class-aware flows along the entire path
    # ---------------------------------------------------------

    def install_path_flows(
        self,
        path,
        src_mac,
        dst_mac,
        ip_pkt,
        tcp_pkt,
        traffic_class,
        priority
    ):
        """Installs matching TCP forwarding rules on every path switch."""

        if not path:
            return False

        if ip_pkt is None or tcp_pkt is None:
            return False

        for index, dpid in enumerate(path):

            datapath = self.datapaths.get(dpid)

            if datapath is None:
                return False

            parser = datapath.ofproto_parser

            # Determine ingress port for this switch.
            if index == 0:

                # The first switch receives from the source host.
                source_port = self.mac_to_port.get(
                    dpid, {}
                ).get(src_mac)

                if source_port is None:
                    return False

                in_port = source_port

            else:

                previous_switch = path[index - 1]

                previous_out_port = self.adjacency.get(
                    previous_switch, {}
                ).get(dpid)

                if previous_out_port is None:
                    return False

                reverse_link = self.reverse_links.get(
                    (previous_switch, previous_out_port)
                )

                if reverse_link is None:
                    return False

                in_port = reverse_link[1]

            # Determine output port for this switch.
            if index == len(path) - 1:

                # Last switch forwards to the destination host.
                out_port = self.mac_to_port.get(
                    dpid, {}
                ).get(dst_mac)

            else:

                next_switch = path[index + 1]

                out_port = self.adjacency.get(
                    dpid, {}
                ).get(next_switch)

            if out_port is None:
                return False

            match = parser.OFPMatch(
                in_port=in_port,
                eth_src=src_mac,
                eth_dst=dst_mac,
                eth_type=ether_types.ETH_TYPE_IP,
                ipv4_src=ip_pkt.src,
                ipv4_dst=ip_pkt.dst,
                ip_proto=6,
                tcp_src=tcp_pkt.src_port,
                tcp_dst=tcp_pkt.dst_port
            )

            actions = [
                parser.OFPActionOutput(out_port)
            ]

            self.add_flow(
                datapath,
                priority,
                match,
                actions
            )

            self.logger.info(
                "PATH FLOW: class=%s switch=%s in=%s out=%s",
                traffic_class,
                dpid,
                in_port,
                out_port
            )

        return True

    # ---------------------------------------------------------
    # Packet handling and forwarding
    # ---------------------------------------------------------

    @set_ev_cls(
        ofp_event.EventOFPPacketIn,
        MAIN_DISPATCHER
    )
    def packet_in_handler(self, ev):
        """Learns MAC addresses, classifies and forwards packets."""

        msg = ev.msg
        datapath = msg.datapath

        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        in_port = msg.match["in_port"]
        dpid = datapath.id

        pkt = packet.Packet(msg.data)

        eth_list = pkt.get_protocols(
            ethernet.ethernet
        )

        if not eth_list:
            return

        eth = eth_list[0]

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return

        dst = eth.dst
        src = eth.src

        ip_pkt = pkt.get_protocol(ipv4.ipv4)

        # Check whether the source or destination IP is blocked.
        if ip_pkt is not None:

            if (
                ip_pkt.src in self.blocked_ips
                or ip_pkt.dst in self.blocked_ips
            ):
                return

        tcp_pkt = pkt.get_protocol(tcp.tcp)

        traffic_class = None
        class_priority = None

        class_field = None
        class_port = None

        if tcp_pkt is not None:

            if tcp_pkt.dst_port in TRAFFIC_CLASSES:

                class_port = tcp_pkt.dst_port
                class_field = "tcp_dst"

            elif tcp_pkt.src_port in TRAFFIC_CLASSES:

                class_port = tcp_pkt.src_port
                class_field = "tcp_src"

            if class_port is not None:

                traffic_class, class_priority = (
                    TRAFFIC_CLASSES[class_port]
                )

        # Learn source MAC on host-facing ports.
        if self.is_host_port(dpid, in_port):

            self.mac_to_port.setdefault(
                dpid,
                {}
            )

            self.mac_to_port[dpid][src] = in_port

        # Find the destination host's switch.
        target_dpid = None

        for switch_id, mac_map in self.mac_to_port.items():

            if dst in mac_map:

                target_dpid = switch_id
                break

        out_port = None
        path = []

        if target_dpid is not None:

            path = self.get_selected_path(
                dpid,
                target_dpid,
                traffic_class
            )

            out_port = self.get_out_port(
                dpid,
                target_dpid,
                dst,
                in_port,
                traffic_class
            )

        # -----------------------------------------------------
        # Unknown destination or unavailable path
        # -----------------------------------------------------

        if out_port is None:

            if (
                target_dpid is not None
                and self.mode == "static"
            ):
                return

            all_ports = [
                port
                for port in datapath.ports.keys()
                if port < ofproto.OFPP_MAX
            ]

            host_facing = [
                port
                for port in all_ports
                if self.is_host_port(dpid, port)
            ]

            spanning_tree_ports = list(
                self.get_spanning_tree_ports(dpid)
            )

            flood_ports = set(
                host_facing + spanning_tree_ports
            )

            if in_port in flood_ports:
                flood_ports.remove(in_port)

            actions = [
                parser.OFPActionOutput(port)
                for port in flood_ports
            ]

        # -----------------------------------------------------
        # Known destination
        # -----------------------------------------------------

        else:

            actions = [
                parser.OFPActionOutput(out_port)
            ]

            if (
                traffic_class is not None
                and class_priority is not None
                and class_field is not None
                and ip_pkt is not None
                and tcp_pkt is not None
            ):

                # Install class-aware rules along the complete path.
                installed = self.install_path_flows(
                    path,
                    src,
                    dst,
                    ip_pkt,
                    tcp_pkt,
                    traffic_class,
                    class_priority
                )

                if not installed:

                    self.logger.warning(
                        "Could not install complete %s path for %s -> %s",
                        traffic_class,
                        src,
                        dst
                    )

            elif tcp_pkt is not None:

                # Other TCP traffic is forwarded with packet-out.
                # It does not receive a generic L2 flow.

                pass

            elif ip_pkt is not None:

                match = parser.OFPMatch(
                    in_port=in_port,
                    eth_dst=dst,
                    eth_type=ether_types.ETH_TYPE_IP,
                    ip_proto=ip_pkt.proto
                )

                self.add_flow(
                    datapath,
                    PRIORITY_L2,
                    match,
                    actions
                )

            else:

                match = parser.OFPMatch(
                    in_port=in_port,
                    eth_dst=dst,
                    eth_type=eth.ethertype
                )

                self.add_flow(
                    datapath,
                    PRIORITY_L2,
                    match,
                    actions
                )

        # -----------------------------------------------------
        # Send the current packet
        # -----------------------------------------------------

        data = None

        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        packet_out = parser.OFPPacketOut(
            datapath=datapath,
            buffer_id=msg.buffer_id,
            in_port=in_port,
            actions=actions,
            data=data
        )

        datapath.send_msg(packet_out)

    # ---------------------------------------------------------
    # Flow statistics monitoring
    # ---------------------------------------------------------

    def _monitor(self):
        """Requests flow statistics every five seconds."""

        self.logger.warning(
            "Flow statistics monitoring started; interval=%d seconds",
            MONITOR_INTERVAL
        )

        while True:

            for datapath in list(self.datapaths.values()):

                try:

                    parser = datapath.ofproto_parser

                    self.logger.warning(
                        "STATS REQUEST SENT TO SWITCH %s",
                        datapath.id
                    )

                    request = parser.OFPFlowStatsRequest(
                        datapath
                    )

                    datapath.send_msg(request)

                except Exception as error:

                    self.logger.error(
                        "Could not request statistics from switch %s: %s",
                        datapath.id,
                        str(error)
                    )

            hub.sleep(MONITOR_INTERVAL)

    # ---------------------------------------------------------
    # Receive flow statistics replies
    # ---------------------------------------------------------

    @set_ev_cls(
        ofp_event.EventOFPFlowStatsReply,
        MAIN_DISPATCHER
    )
    def flow_stats_reply_handler(self, ev):
        """Collects HIGH and NORMAL flow counters."""

        dpid = ev.msg.datapath.id

        self.logger.warning(
            "STATS REPLY RECEIVED FROM SWITCH %s. Entries=%d",
            dpid,
            len(ev.msg.body)
        )

        high_count = 0
        normal_count = 0
        matched_flows = 0

        for stat in ev.msg.body:

            if stat.priority not in (100, 50):
                continue

            if "tcp_dst" in stat.match:
                field = "tcp_dst"

            elif "tcp_src" in stat.match:
                field = "tcp_src"

            else:
                continue

            tcp_port = stat.match[field]

            class_info = TRAFFIC_CLASSES.get(
                tcp_port
            )

            if class_info is None:
                continue

            traffic_class, expected_priority = class_info

            if stat.priority != expected_priority:
                continue

            matched_flows += 1

            key = (
                dpid,
                stat.priority,
                str(stat.match)
            )

            previous = self.last_counts.get(
                key,
                0
            )

            current = stat.packet_count

            if current > previous:
                packet_increase = current - previous

            elif current < previous:
                packet_increase = current

            else:
                packet_increase = 0

            self.logger.warning(
                "FLOW DEBUG: switch=%s priority=%s packets=%s match=%s",
                dpid,
                stat.priority,
                current,
                stat.match
            )

            if packet_increase > 0:

                self.logger.warning(
                    "CLASS=%s switch=%s %s=%s packets=%s (+%s)",
                    traffic_class,
                    dpid,
                    field,
                    tcp_port,
                    current,
                    packet_increase
                )

            self.last_counts[key] = current

            if traffic_class == "HIGH":
                high_count += current

            elif traffic_class == "NORMAL":
                normal_count += current

        self.class_counts_by_switch["HIGH"][dpid] = high_count

        self.class_counts_by_switch["NORMAL"][dpid] = normal_count

        self.class_totals["HIGH"] = sum(
            self.class_counts_by_switch["HIGH"].values()
        )

        self.class_totals["NORMAL"] = sum(
            self.class_counts_by_switch["NORMAL"].values()
        )

        self.logger.warning(
            "STATS SUMMARY: switch=%s matched_flows=%s HIGH=%s NORMAL=%s",
            dpid,
            matched_flows,
            high_count,
            normal_count
        )


# =============================================================
# REST API
# =============================================================

class PubSubRestController(ControllerBase):
    """REST API for subscriptions, statistics, blocking and modes."""

    def __init__(self, req, link, data, **config):

        super(PubSubRestController, self).__init__(
            req,
            link,
            data,
            **config
        )

        self.app = data['pubsub_app']
        self.pubsub_app = self.app

    # ---------------------------------------------------------
    # Subscription endpoint
    # ---------------------------------------------------------

    @route(
        'pubsub',
        '/subscription',
        methods=['POST']
    )
    def handle_subscription(self, req, **kwargs):
        """Adds or removes an IP address from a topic."""

        try:

            body = json.loads(
                req.body.decode('utf-8')
            )

            topic = body.get('topic')
            ip = body.get('ip')

            action = body.get(
                'action',
                'subscribe'
            ).lower()

            if not topic or not ip:

                return json.dumps({
                    "status": "error",
                    "message": "Both topic and IP are required"
                })

            if action == "subscribe":

                self.pubsub_app.subscriptions.setdefault(
                    topic,
                    []
                )

                if ip not in self.pubsub_app.subscriptions[topic]:

                    self.pubsub_app.subscriptions[
                        topic
                    ].append(ip)

                self.pubsub_app.logger.info(
                    "REST API: Subscribed %s to %s",
                    ip,
                    topic
                )

                return json.dumps({
                    "status": "success",
                    "action": "subscribe",
                    "subscriptions":
                        self.pubsub_app.subscriptions
                })

            elif action == "unsubscribe":

                if topic in self.pubsub_app.subscriptions:

                    if ip in self.pubsub_app.subscriptions[topic]:

                        self.pubsub_app.subscriptions[
                            topic
                        ].remove(ip)

                    if not self.pubsub_app.subscriptions[topic]:
                        del self.pubsub_app.subscriptions[topic]

                self.pubsub_app.logger.info(
                    "REST API: Unsubscribed %s from %s",
                    ip,
                    topic
                )

                return json.dumps({
                    "status": "success",
                    "action": "unsubscribe",
                    "subscriptions":
                        self.pubsub_app.subscriptions
                })

            else:

                return json.dumps({
                    "status": "error",
                    "message":
                        "Action must be subscribe or unsubscribe"
                })

        except Exception as error:

            return json.dumps({
                "status": "error",
                "message": str(error)
            })

    # ---------------------------------------------------------
    # Get subscriptions
    # ---------------------------------------------------------

    @route(
        'pubsub',
        '/subscriptions',
        methods=['GET']
    )
    def get_subscriptions(self, req, **kwargs):
        """Returns all topic subscriptions."""

        return json.dumps(
            self.pubsub_app.subscriptions
        )

    # ---------------------------------------------------------
    # Mode endpoint
    # ---------------------------------------------------------

    @route(
        'pubsub',
        '/mode',
        methods=['POST']
    )
    def set_mode(self, req, **kwargs):
        """Changes the controller between static and dynamic modes."""

        try:

            body = json.loads(
                req.body.decode('utf-8')
            )

            new_mode = body.get('mode')

            if new_mode not in ("static", "dynamic"):

                return json.dumps({
                    "status": "error",
                    "message": "Use 'static' or 'dynamic'"
                })

            self.pubsub_app.mode = new_mode

            self.pubsub_app.static_paths_cache.clear()

            if new_mode == "static":

                self.pubsub_app.build_static_path_cache()

            self.pubsub_app.flush_all_switches()

            self.pubsub_app.logger.info(
                "REST API: Controller mode set to %s",
                new_mode
            )

            return json.dumps({
                "status": "success",
                "mode": self.pubsub_app.mode
            })

        except Exception as error:

            return json.dumps({
                "status": "error",
                "message": str(error)
            })

    # ---------------------------------------------------------
    # Statistics endpoint
    # ---------------------------------------------------------

    @route(
        'pubsub',
        '/stats',
        methods=['GET']
    )
    def get_stats(self, req, **kwargs):
        """Returns traffic and topology statistics."""

        app = self.pubsub_app

        link_list = []

        for link_key, link_info in app.known_links.items():

            status = app.link_status.get(
                link_key,
                "DOWN"
            )

            link_list.append({
                "source": "s" + str(link_info["source"]),
                "destination": "s" + str(link_info["destination"]),
                "status": status
            })

        stats_data = {

            "mode": app.mode,

            "switches": list(
                app.datapaths.keys()
            ),

            "links": link_list,

            "classes": {
                "HIGH": app.class_totals["HIGH"],
                "NORMAL": app.class_totals["NORMAL"]
            },

            "flow_counts": {
                "HIGH": app.class_totals["HIGH"],
                "NORMAL": app.class_totals["NORMAL"]
            },

            "subscriptions": app.subscriptions,

            "blocked_ips": list(
                app.blocked_ips
            )
        }

        return json.dumps(stats_data)

    # ---------------------------------------------------------
    # Block IP endpoint
    # ---------------------------------------------------------

    @route(
        'pubsub',
        '/block',
        methods=['POST']
    )
    def block_ip(self, req, **kwargs):
        """Blocks an IP by installing drop rules on every switch."""

        try:

            body = json.loads(
                req.body.decode('utf-8')
            )

            ip_to_block = body.get('ip')

            if not ip_to_block:

                return json.dumps({
                    "status": "error",
                    "message": "No IP provided"
                })

            self.pubsub_app.blocked_ips.add(
                ip_to_block
            )

            for datapath in list(
                self.pubsub_app.datapaths.values()
            ):

                self.pubsub_app.install_block_rules(
                    datapath
                )

            self.pubsub_app.logger.info(
                "REST API: Blocked IP address: %s",
                ip_to_block
            )

            return json.dumps({
                "status": "success",
                "blocked_ip": ip_to_block
            })

        except Exception as error:

            return json.dumps({
                "status": "error",
                "message": str(error)
            })
