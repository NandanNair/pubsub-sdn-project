
"""
Ryu OpenFlow 1.3 Controller for Pub-Sub SDN Project (Deliverable 2).

Features:
- MAC learning
- TCP traffic classification:
    Port 5001 -> HIGH priority 100
    Port 5002 -> NORMAL priority 50
- Static and dynamic routing modes
- BFS shortest-path routing
- BFS spanning-tree flooding to prevent loops
- Link failure detection and recovery
- Flow-table cleanup and route recalculation
- Periodic flow statistics monitoring
- REST API for subscriptions, mode, statistics and IP blocking
"""

import json

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.lib import hub
from ryu.lib.packet import ether_types, ethernet, packet, ipv4
from ryu.ofproto import ofproto_v1_3
from ryu.topology import event as topo_event
from ryu.topology.api import get_switch, get_link
from ryu.app.wsgi import ControllerBase, WSGIApplication, route



TRAFFIC_CLASSES = {
    5001: ("HIGH", 100),
    5002: ("NORMAL", 50),
}

PRIORITY_L2 = 1
PRIORITY_TABLE_MISS = 0
MONITOR_INTERVAL = 5


class PubSubController(app_manager.RyuApp):

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {'wsgi': WSGIApplication}

    def __init__(self, *args, **kwargs):
        super(PubSubController, self).__init__(*args, **kwargs)

        self.mac_to_port = {}
        self.datapaths = {}
        self.last_counts = {}

        # Topology information
        self.switches = []
        self.links = {}
        self.reverse_links = {}
        self.adjacency = {}
        self.down_ports = set()

        # Application information
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

    # ---------------------------------------------------------
    # Switch connection and flow installation
    # ---------------------------------------------------------

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        """Registers a switch and installs its table-miss rule."""

        datapath = ev.msg.datapath
        self.datapaths[datapath.id] = datapath

        self.install_table_miss(datapath)

        self.logger.info(
            "Switch %s connected",
            datapath.id
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

    def add_flow(self, datapath, priority, match, actions,
                 buffer_id=None):
        """Creates and installs an OpenFlow flow entry."""

        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        instructions = [
            parser.OFPInstructionActions(
                ofproto.OFPIT_APPLY_ACTIONS,
                actions
            )
        ]

        if buffer_id is not None and buffer_id != ofproto.OFP_NO_BUFFER:
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
    # Traffic classification
    # ---------------------------------------------------------

    def install_class_rules(self, datapath, mac, port):
        """Installs HIGH and NORMAL TCP classification rules."""

        if self.mode == "static":
            return

        parser = datapath.ofproto_parser
        actions = [parser.OFPActionOutput(port)]

        for tcp_port, (traffic_class, priority) in TRAFFIC_CLASSES.items():

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
        """Rebuilds the switch adjacency graph using discovered links."""

        switch_list = get_switch(self, None)
        link_list = get_link(self, None)

        self.switches = [switch.dp.id for switch in switch_list]

        self.links.clear()
        self.reverse_links.clear()
        self.adjacency.clear()

        for switch_id in self.switches:
            self.adjacency[switch_id] = {}

        for link in link_list:

            src_dpid = link.src.dpid
            dst_dpid = link.dst.dpid

            src_port = link.src.port_no
            dst_port = link.dst.port_no

            # Ignore links containing a port marked as down.
            if (src_dpid, src_port) in self.down_ports:
                continue

            if (dst_dpid, dst_port) in self.down_ports:
                continue

            self.links[(src_dpid, dst_dpid)] = src_port

            self.reverse_links[(src_dpid, src_port)] = (
                dst_dpid,
                dst_port
            )

            self.adjacency.setdefault(src_dpid, {})
            self.adjacency[src_dpid][dst_dpid] = src_port

        self.static_paths_cache.clear()

        self.logger.info(
            "Topology refreshed: switches=%s, directed links=%d",
            self.switches,
            len(self.links)
        )

    @set_ev_cls([
        topo_event.EventSwitchEnter,
        topo_event.EventSwitchLeave,
        topo_event.EventLinkAdd,
        topo_event.EventLinkDelete
    ], MAIN_DISPATCHER)
    def topology_change_handler(self, ev):
        """Updates topology and forwarding rules after topology events."""

        self.refresh_topology()
        self.flush_all_switches()

    # ---------------------------------------------------------
    # Link failure and recovery
    # ---------------------------------------------------------

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def port_status_handler(self, ev):
        """Handles port-down and port-up events on switches."""

        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto

        dpid = datapath.id
        port_no = msg.desc.port_no

        # Find the remote end before changing topology information.
        remote_link = self.reverse_links.get((dpid, port_no))

        is_down = bool(
            (msg.desc.state & ofproto.OFPPS_LINK_DOWN) or
            (msg.desc.config & ofproto.OFPPC_PORT_DOWN) or
            (msg.reason == ofproto.OFPPR_DELETE)
        )

        changed = False

        if is_down:

            if (dpid, port_no) not in self.down_ports:
                self.down_ports.add((dpid, port_no))
                changed = True

            # Mark the other end of the inter-switch link as down too.
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

            # Restore the other end when the link comes back.
            if remote_link is not None:
                remote_dpid, remote_port = remote_link

                if (remote_dpid, remote_port) in self.down_ports:
                    self.down_ports.remove(
                        (remote_dpid, remote_port)
                    )
                    changed = True

        if changed:

            self.logger.info(
                "Port status changed: switch=%d port=%d down=%s",
                dpid,
                port_no,
                is_down
            )

            self.static_paths_cache.clear()

            # Rebuild the graph without the failed link.
            self.refresh_topology()

            # Remove old forwarding rules and install fresh rules.
            self.flush_all_switches()

    # ---------------------------------------------------------
    # Flow-table cleanup
    # ---------------------------------------------------------

    def flush_all_switches(self):
        """
        Deletes stale forwarding rules from connected switches.
        Then reinstalls table-miss and learned-host classification rules.
        """

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

            # Restore the table-miss rule after deleting old flows.
            self.install_table_miss(datapath)

            # Restore classification rules for already learned hosts.
            for mac, port in self.mac_to_port.get(
                datapath.id, {}
            ).items():

                self.install_class_rules(
                    datapath,
                    mac,
                    port
                )

        self.last_counts.clear()

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
            port_no not in inter_switch_ports and
            (dpid, port_no) not in self.down_ports
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

        for neighbor, port in self.adjacency.get(dpid, {}).items():

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

            for neighbor in self.adjacency.get(current, {}):

                if neighbor not in visited:

                    visited.add(neighbor)

                    new_path = list(path)
                    new_path.append(neighbor)

                    queue.append(new_path)

        return []

    def get_out_port(self, src_dpid, dst_dpid, dst_mac, in_port):
        """Returns the next-hop output port for a destination."""

        if src_dpid == dst_dpid:

            if dst_mac in self.mac_to_port.get(src_dpid, {}):
                return self.mac_to_port[src_dpid][dst_mac]

            return None

        cache_key = (src_dpid, dst_dpid)

        if (
            self.mode == "static" and
            cache_key in self.static_paths_cache
        ):
            path = self.static_paths_cache[cache_key]

        else:

            path = self.get_shortest_path(
                src_dpid,
                dst_dpid
            )

            if self.mode == "static":
                self.static_paths_cache[cache_key] = path

        if len(path) > 1:

            next_hop = path[1]

            return self.adjacency.get(
                src_dpid, {}
            ).get(next_hop)

        return None

    # ---------------------------------------------------------
    # Packet handling and forwarding
    # ---------------------------------------------------------

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        """Learns host MACs and forwards packets using available paths."""

        msg = ev.msg
        datapath = msg.datapath

        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        in_port = msg.match["in_port"]
        dpid = datapath.id

        pkt = packet.Packet(msg.data)
        eth_list = pkt.get_protocols(ethernet.ethernet)

        if not eth_list:
            return

        eth = eth_list[0]

        # Ignore LLDP packets used for topology discovery.
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return

        dst = eth.dst
        src = eth.src

        # Drop packets from blocked source IP addresses.
        ip_pkt = pkt.get_protocol(ipv4.ipv4)

        if ip_pkt and ip_pkt.src in self.blocked_ips:
            return

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
                in_port
            )

        # Unknown destination or no available path:
        # flood only over host ports and spanning-tree ports.
        if out_port is None:

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

        else:

            actions = [
                parser.OFPActionOutput(out_port)
            ]

            match = parser.OFPMatch(
                in_port=in_port,
                eth_dst=dst
            )

            if msg.buffer_id != ofproto.OFP_NO_BUFFER:

                self.add_flow(
                    datapath,
                    PRIORITY_L2,
                    match,
                    actions,
                    msg.buffer_id
                )

                return

            self.add_flow(
                datapath,
                PRIORITY_L2,
                match,
                actions
            )

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

        while True:

            for datapath in list(self.datapaths.values()):

                parser = datapath.ofproto_parser

                request = parser.OFPFlowStatsRequest(
                    datapath
                )

                datapath.send_msg(request)

            hub.sleep(MONITOR_INTERVAL)

    @set_ev_cls(
        ofp_event.EventOFPFlowStatsReply,
        MAIN_DISPATCHER
    )
    def flow_stats_reply_handler(self, ev):
        """Reads flow counters and logs HIGH/NORMAL traffic activity."""

        dpid = ev.msg.datapath.id

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

            traffic_class = TRAFFIC_CLASSES.get(
                tcp_port,
                ("UNKNOWN", 0)
            )[0]

            key = (
                dpid,
                stat.priority,
                str(stat.match)
            )

            previous = self.last_counts.get(key, 0)

            if stat.packet_count > previous:

                self.logger.info(
                    "CLASS=%s switch=%d %s=%d -> %s packets=%d (+%d)",
                    traffic_class,
                    dpid,
                    field,
                    tcp_port,
                    stat.match.get("eth_dst", "?"),
                    stat.packet_count,
                    stat.packet_count - previous
                )

            self.last_counts[key] = stat.packet_count


# =============================================================
# REST API
# =============================================================

class PubSubRestController(ControllerBase):
    """REST API for subscriptions, statistics, blocking and modes."""

    def __init__(self, req, link, data, **config):
        super(PubSubRestController, self).__init__(
            req, link, data, **config
        )

        self.app = data['pubsub_app']
        self.pubsub_app = self.app

    @route('pubsub', '/subscription', methods=['POST'])
    def handle_subscription(self, req, **kwargs):
        """Adds an IP address to a topic subscription."""

        try:

            body = json.loads(
                req.body.decode('utf-8')
            )

            topic = body.get('topic')
            ip = body.get('ip')

            if not topic or not ip:
                return json.dumps({
                    "status": "error",
                    "message": "Both topic and IP are required"
                })

            self.pubsub_app.subscriptions.setdefault(
                topic,
                []
            )

            if ip not in self.pubsub_app.subscriptions[topic]:
                self.pubsub_app.subscriptions[topic].append(ip)

            self.pubsub_app.logger.info(
                "REST API: Subscription added: %s",
                body
            )

            return json.dumps({
                "status": "success",
                "subscriptions": self.pubsub_app.subscriptions
            })

        except Exception as e:

            return json.dumps({
                "status": "error",
                "message": str(e)
            })

    @route('pubsub', '/subscriptions', methods=['GET'])
    def get_subscriptions(self, req, **kwargs):
        """Returns all topic subscriptions."""

        return json.dumps(
            self.pubsub_app.subscriptions
        )

    @route('pubsub', '/mode', methods=['POST'])
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

            # Reinstall rules according to the selected mode.
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

    @route('pubsub', '/stats', methods=['GET'])
    def get_stats(self, req, **kwargs):
        """Returns controller and network statistics."""

        stats_data = {
            "mode": self.pubsub_app.mode,
            "switches": list(
                self.pubsub_app.datapaths.keys()
            ),
            "links": list(
                self.pubsub_app.links.keys()
            ),
            "flow_counts": {
                str(key): value
                for key, value in self.pubsub_app.last_counts.items()
            },
            "subscriptions": self.pubsub_app.subscriptions,
            "blocked_ips": list(
                self.pubsub_app.blocked_ips
            )
        }

        return json.dumps(stats_data)

    @route('pubsub', '/block', methods=['POST'])
    def block_ip(self, req, **kwargs):
        """Adds an IP address to the blocked-IP list."""

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
