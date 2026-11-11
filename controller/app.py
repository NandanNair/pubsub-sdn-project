
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
    _CONTEXTS = {'wsgi': WSGIApplication}

    def __init__(self, *args, **kwargs):
        super(PubSubController, self).__init__(*args, **kwargs)

        self.mac_to_port = {}
        self.datapaths = {}

        # Stores the previous packet count of each monitored flow.
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
        """Registers a switch and clears stale state."""

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
            "Switch %s connected; stale state cleared",
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
        """Installs high-priority drop rules for blocked IPs."""

        parser = datapath.ofproto_parser

        for ip in self.blocked_ips:

            match = parser.OFPMatch(
                eth_type=ether_types.ETH_TYPE_IP,
                ipv4_src=ip
            )

            self.add_flow(
                datapath,
                PRIORITY_BLOCK,
                match,
                []
            )

    # ---------------------------------------------------------
    # Traffic classification
    # ---------------------------------------------------------

    def install_class_rules(self, datapath, mac, port):
        """Installs classification rules for learned host MACs."""

        if self.mode == "static":
            return

        parser = datapath.ofproto_parser

        actions = [
            parser.OFPActionOutput(port)
        ]

        for tcp_port, class_info in TRAFFIC_CLASSES.items():

            traffic_class, priority = class_info

            for field in ("tcp_dst", "tcp_src"):

                match = parser.OFPMatch(
                    eth_type=ether_types.ETH_TYPE_IP,
                    ip_proto=6,
                    eth_dst=mac,
                    **{field: tcp_port}
                )

                self.add_flow(
                    datapath,
                    priority,
                    match,
                    actions
                )

        self.logger.info(
            "Class rules installed for %s through port %d on switch %d",
            mac,
            port,
            datapath.id
        )

    # ---------------------------------------------------------
    # Topology discovery
    # ---------------------------------------------------------

    def refresh_topology(self):
        """Rebuilds active topology while retaining known links."""

        switch_list = get_switch(self, None)
        link_list = get_link(self, None)

        self.switches = [
            switch.dp.id for switch in switch_list
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

                self.links[(src_dpid, dst_dpid)] = src_port

                self.reverse_links[
                    (src_dpid, src_port)
                ] = (dst_dpid, dst_port)

                self.adjacency.setdefault(src_dpid, {})
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
        """Updates topology and reroutes only in dynamic mode."""

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
                self.down_ports.add((dpid, port_no))
                changed = True

            if remote_link is not None:

                remote_dpid, remote_port = remote_link

                if (remote_dpid, remote_port) not in self.down_ports:
                    self.down_ports.add(
                        (remote_dpid, remote_port)
                    )
                    changed = True

        else:

            if (dpid, port_no) in self.down_ports:
                self.down_ports.remove((dpid, port_no))
                changed = True

            if remote_link is not None:

                remote_dpid, remote_port = remote_link

                if (remote_dpid, remote_port) in self.down_ports:
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
        """Deletes stale flows and reinstalls essential rules."""

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

            for mac, port in self.mac_to_port.get(
                datapath.id, {}
            ).items():

                self.install_class_rules(
                    datapath,
                    mac,
                    port
                )

        # Clear previous counter values so new flows
        # are monitored from the beginning.
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
            "Stale flow rules removed and essential rules reinstalled"
        )

    # ---------------------------------------------------------
    # Host-port identification and path calculation
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

    def get_spanning_tree_ports(self, dpid):
        """Finds loop-free switch ports using BFS spanning-tree logic."""

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

                    tree_edges.add((current, neighbor))
                    tree_edges.add((neighbor, current))

                    queue.append(neighbor)

        tree_ports = set()

        for neighbor, port in self.adjacency.get(
            dpid, {}
        ).items():

            if (dpid, neighbor) in tree_edges:
                tree_ports.add(port)

        return tree_ports

    def get_shortest_path(self, src_dpid, dst_dpid):
        """Finds the shortest available switch path using BFS."""

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

    def build_static_path_cache(self):
        """Stores initial shortest paths for static-mode forwarding."""

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

    def get_class_path(
        self,
        src_dpid,
        dst_dpid,
        traffic_class
    ):
        """Selects preferred paths for HIGH and NORMAL traffic."""

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
                return [SWITCH_1, SWITCH_3]

            if (
                src_dpid == SWITCH_3
                and dst_dpid == SWITCH_1
                and SWITCH_1 in self.adjacency.get(
                    SWITCH_3, {}
                )
            ):
                return [SWITCH_3, SWITCH_1]

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

    def get_out_port(
        self,
        src_dpid,
        dst_dpid,
        dst_mac,
        in_port,
        traffic_class=None
    ):
        """Returns the next-hop port using the selected path."""

        if src_dpid == dst_dpid:

            if dst_mac in self.mac_to_port.get(
                src_dpid, {}
            ):
                return self.mac_to_port[src_dpid][dst_mac]

            return None

        if self.mode == "static":

            cache_key = (src_dpid, dst_dpid)

            if cache_key not in self.static_paths_cache:

                path = self.get_shortest_path(
                    src_dpid,
                    dst_dpid
                )

                self.static_paths_cache[cache_key] = path

            else:
                path = self.static_paths_cache[cache_key]

        else:

            path = self.get_class_path(
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
    # Packet handling and forwarding
    # ---------------------------------------------------------

    @set_ev_cls(
        ofp_event.EventOFPPacketIn,
        MAIN_DISPATCHER
    )
    def packet_in_handler(self, ev):
        """Learns MAC addresses, classifies traffic and forwards packets."""

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

        # Drop packets from blocked source IP addresses.
        ip_pkt = pkt.get_protocol(ipv4.ipv4)

        if ip_pkt and ip_pkt.src in self.blocked_ips:
            return

        # Identify traffic class using TCP ports.
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

        # Learn source MAC only on host-facing ports.
        if self.is_host_port(dpid, in_port):

            self.mac_to_port.setdefault(dpid, {})

            if self.mac_to_port[dpid].get(src) != in_port:

                self.mac_to_port[dpid][src] = in_port

                self.install_class_rules(
                    datapath,
                    src,
                    in_port
                )

        # Find the switch where the destination MAC is learned.
        target_dpid = None

        for switch_id, mac_map in self.mac_to_port.items():

            if dst in mac_map:

                target_dpid = switch_id
                break

        out_port = None

        if target_dpid is not None:

            out_port = self.get_out_port(
                dpid,
                target_dpid,
                dst,
                in_port,
                traffic_class
            )

        # Unknown destination or unavailable path.
        if out_port is None:

            # Do not flood around a failed known path in static mode.
            if (
                target_dpid is not None
                and self.mode == "static"
            ):
                return

            all_ports = [
                port for port in datapath.ports.keys()
                if port < ofproto.OFPP_MAX
            ]

            host_facing = [
                port for port in all_ports
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

            # Flooded packets use packet-out.
            # No generic L2 flow is installed here.

        else:

            actions = [
                parser.OFPActionOutput(out_port)
            ]

            if (
                class_priority is not None
                and class_field is not None
            ):

                # Classified TCP traffic gets a class-specific flow.
                match = parser.OFPMatch(
                    in_port=in_port,
                    eth_dst=dst,
                    eth_type=ether_types.ETH_TYPE_IP,
                    ip_proto=6,
                    **{class_field: class_port}
                )

                flow_priority = class_priority

            elif tcp_pkt is not None:

                # Other TCP traffic must not install a generic L2 flow.
                # Forward the current packet using packet-out.
                match = None
                flow_priority = None

            elif ip_pkt is not None:

                # Non-TCP IPv4 traffic gets a protocol-specific flow.
                match = parser.OFPMatch(
                    in_port=in_port,
                    eth_dst=dst,
                    eth_type=ether_types.ETH_TYPE_IP,
                    ip_proto=ip_pkt.proto
                )

                flow_priority = PRIORITY_L2

            else:

                # ARP and other non-IP traffic get an
                # Ethernet-type-specific flow.
                match = parser.OFPMatch(
                    in_port=in_port,
                    eth_dst=dst,
                    eth_type=eth.ethertype
                )

                flow_priority = PRIORITY_L2

            # Install a flow only when a suitable match exists.
            if match is not None:

                if msg.buffer_id != ofproto.OFP_NO_BUFFER:

                    self.add_flow(
                        datapath,
                        flow_priority,
                        match,
                        actions,
                        msg.buffer_id
                    )

                    return

                self.add_flow(
                    datapath,
                    flow_priority,
                    match,
                    actions
                )

        # Send the current packet.
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
        """Requests flow statistics from every switch every five seconds."""

        self.logger.info(
            "Flow statistics monitoring started; interval=%d seconds",
            MONITOR_INTERVAL
        )

        while True:

            for datapath in list(
                self.datapaths.values()
            ):

                try:

                    parser = datapath.ofproto_parser

                    request = parser.OFPFlowStatsRequest(
                        datapath
                    )

                    datapath.send_msg(request)

                except Exception as error:

                    self.logger.error(
                        "Could not request flow statistics from switch %d: %s",
                        datapath.id,
                        str(error)
                    )

            hub.sleep(MONITOR_INTERVAL)

    @set_ev_cls(
        ofp_event.EventOFPFlowStatsReply,
        MAIN_DISPATCHER
    )
    def flow_stats_reply_handler(self, ev):
        """Collects HIGH and NORMAL flow counters and logs increases."""

        dpid = ev.msg.datapath.id

        high_count = 0
        normal_count = 0
        matched_flows = 0

        for stat in ev.msg.body:

            # Only inspect the HIGH and NORMAL priority rules.
            if stat.priority not in (100, 50):
                continue

            # Identify the TCP port field in the flow match.
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

            # Ensure that the priority corresponds to the class.
            if stat.priority != expected_priority:
                continue

            matched_flows += 1

            key = (
                dpid,
                stat.priority,
                str(stat.match)
            )

            previous = self.last_counts.get(key, 0)
            current = stat.packet_count

            # Calculate the number of newly observed packets.
            # If a flow counter resets, treat its current value
            # as the new packet increase.
            if current > previous:
                packet_increase = current - previous

            elif current < previous:
                packet_increase = current

            else:
                packet_increase = 0

            if packet_increase > 0:

                self.logger.info(
                    "CLASS=%s switch=%d %s=%d packets=%d (+%d)",
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

        # Update the per-switch statistics.
        self.class_counts_by_switch["HIGH"][dpid] = high_count

        self.class_counts_by_switch["NORMAL"][dpid] = normal_count

        # Calculate totals across all switches.
        self.class_totals["HIGH"] = sum(
            self.class_counts_by_switch["HIGH"].values()
        )

        self.class_totals["NORMAL"] = sum(
            self.class_counts_by_switch["NORMAL"].values()
        )

        self.logger.debug(
            "Statistics reply: switch=%d matched_class_flows=%d",
            dpid,
            matched_flows
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

                if (
                    ip not in
                    self.pubsub_app.subscriptions[topic]
                ):

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

        except Exception as e:

            return json.dumps({
                "status": "error",
                "message": str(e)
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

        except Exception as e:

            return json.dumps({
                "status": "error",
                "message": str(e)
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

        except Exception as e:

            return json.dumps({
                "status": "error",
                "message": str(e)
            })
