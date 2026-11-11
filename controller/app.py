
import json

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import (
    CONFIG_DISPATCHER,
    MAIN_DISPATCHER,
    set_ev_cls
)
from ryu.lib import hub
from ryu.lib.packet import ether_types, ethernet, packet, ipv4
from ryu.ofproto import ofproto_v1_3
from ryu.topology import event as topo_event
from ryu.topology.api import get_switch, get_link
from ryu.app.wsgi import ControllerBase, WSGIApplication, route


PUBSUB_INSTANCE_NAME = "pubsub_api"

TRAFFIC_CLASSES = {
    5001: ("HIGH", 100),
    5002: ("NORMAL", 50)
}

PRIORITY_L2 = 1
PRIORITY_TABLE_MISS = 0
MONITOR_INTERVAL = 5


class PubSubController(app_manager.RyuApp):

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    _CONTEXTS = {"wsgi": WSGIApplication}

    def __init__(self, *args, **kwargs):
        super(PubSubController, self).__init__(*args, **kwargs)

        # MAC address learning
        self.mac_to_port = {}

        # Connected switches
        self.datapaths = {}

        # Monitoring
        self.last_counts = {}

        # Topology information
        self.switches = []
        self.adjacency = {}
        self.port_to_peer = {}
        self.down_ports = set()

        # Application and REST information
        self.subscriptions = {}
        self.blocked_ips = set()
        self.mode = "dynamic"
        self.static_paths_cache = {}

        # Register REST controller
        wsgi = kwargs["wsgi"]
        wsgi.register(
            PubSubRestController,
            {PUBSUB_INSTANCE_NAME: self}
        )

        # Start background monitoring
        self.monitor_thread = hub.spawn(self._monitor)

    # ---------------------------------------------------------
    # 1. Switch connection and table-miss rule
    # ---------------------------------------------------------

    @set_ev_cls(
        ofp_event.EventOFPSwitchFeatures,
        CONFIG_DISPATCHER
    )
    def switch_features_handler(self, ev):

        datapath = ev.msg.datapath
        self.datapaths[datapath.id] = datapath

        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto

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

        self.logger.info(
            "Switch %s connected",
            datapath.id
        )

    # ---------------------------------------------------------
    # 2. General flow installation helper
    # ---------------------------------------------------------

    def add_flow(
        self,
        datapath,
        priority,
        match,
        actions,
        buffer_id=None
    ):

        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto

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
    # 3. Install HIGH and NORMAL classification rules
    # ---------------------------------------------------------

    def install_class_rules(self, datapath, mac, port):

        if self.mode == "static":
            return

        parser = datapath.ofproto_parser

        actions = [
            parser.OFPActionOutput(port)
        ]

        for tcp_port, (traffic_class, priority) in (
            TRAFFIC_CLASSES.items()
        ):

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
            "Classification rules installed for %s on switch %s",
            mac,
            datapath.id
        )

    # ---------------------------------------------------------
    # 4. Rebuild topology from discovered active links
    # ---------------------------------------------------------

    def rebuild_topology(self):

        switch_list = get_switch(self, None)

        self.switches = sorted(
            [switch.dp.id for switch in switch_list]
        )

        link_list = get_link(self, None)

        new_adjacency = {
            switch_id: {}
            for switch_id in self.switches
        }

        for link in link_list:

            src = link.src.dpid
            dst = link.dst.dpid

            src_port = link.src.port_no
            dst_port = link.dst.port_no

            # Remember both ends, including links that
            # may subsequently go down.
            self.port_to_peer[(src, src_port)] = (
                dst,
                dst_port
            )

            self.port_to_peer[(dst, dst_port)] = (
                src,
                src_port
            )

            # Ignore failed ports
            if (
                (src, src_port) in self.down_ports
                or (dst, dst_port) in self.down_ports
            ):
                continue

            new_adjacency.setdefault(src, {})
            new_adjacency.setdefault(dst, {})

            new_adjacency[src][dst] = src_port
            new_adjacency[dst][src] = dst_port

        self.adjacency = new_adjacency

        self.static_paths_cache.clear()

        # Remove MAC entries learned on ports that are
        # no longer valid host-facing ports.
        for dpid in list(self.mac_to_port.keys()):

            for mac, port in list(
                self.mac_to_port[dpid].items()
            ):

                if not self.is_host_port(dpid, port):
                    del self.mac_to_port[dpid][mac]

        self.logger.info(
            "Topology updated. Switches=%s Adjacency=%s",
            self.switches,
            self.adjacency
        )

    # ---------------------------------------------------------
    # 5. Topology change events
    # ---------------------------------------------------------

    @set_ev_cls([
        topo_event.EventSwitchEnter,
        topo_event.EventSwitchLeave,
        topo_event.EventLinkAdd,
        topo_event.EventLinkDelete
    ])
    def topology_change_handler(self, ev):

        self.rebuild_topology()

        # A topology change can invalidate old routes.
        self.flush_all_switches()

    # ---------------------------------------------------------
    # 6. Handle switch port up/down events
    # ---------------------------------------------------------

    @set_ev_cls(
        ofp_event.EventOFPPortStatus,
        MAIN_DISPATCHER
    )
    def port_status_handler(self, ev):

        msg = ev.msg
        datapath = msg.datapath

        dpid = datapath.id
        ofproto = datapath.ofproto

        port_no = msg.desc.port_no

        is_down = (
            bool(msg.desc.state & ofproto.OFPPS_LINK_DOWN)
            or bool(msg.desc.config & ofproto.OFPPC_PORT_DOWN)
        )

        local_port = (dpid, port_no)

        # Find the other end of the link, if known.
        remote_port = self.port_to_peer.get(local_port)

        changed = False

        if is_down:

            if local_port not in self.down_ports:
                self.down_ports.add(local_port)
                changed = True

            if (
                remote_port is not None
                and remote_port not in self.down_ports
            ):
                self.down_ports.add(remote_port)
                changed = True

        else:

            if local_port in self.down_ports:
                self.down_ports.remove(local_port)
                changed = True

            if (
                remote_port is not None
                and remote_port in self.down_ports
            ):
                self.down_ports.remove(remote_port)
                changed = True

        if changed:

            self.logger.warning(
                "Port state changed: switch=%s port=%s down=%s",
                dpid,
                port_no,
                is_down
            )

            self.rebuild_topology()
            self.flush_all_switches()

    # ---------------------------------------------------------
    # 7. Delete stale flows and reinstall default rules
    # ---------------------------------------------------------

    def flush_all_switches(self):

        for datapath in list(self.datapaths.values()):

            ofproto = datapath.ofproto
            parser = datapath.ofproto_parser

            # Delete all flow entries on the switch.
            # The table-miss rule is installed again below.
            delete_msg = parser.OFPFlowMod(
                datapath=datapath,
                command=ofproto.OFPFC_DELETE,
                out_port=ofproto.OFPP_ANY,
                out_group=ofproto.OFPG_ANY,
                match=parser.OFPMatch()
            )

            datapath.send_msg(delete_msg)

            # Reinstall table-miss rule.
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

            # Reinstall class rules for known local hosts.
            for mac, port in self.mac_to_port.get(
                datapath.id, {}
            ).items():

                if self.is_host_port(datapath.id, port):
                    self.install_class_rules(
                        datapath,
                        mac,
                        port
                    )

        self.logger.info(
            "Stale flow rules flushed. New paths will be learned."
        )

    # ---------------------------------------------------------
    # 8. Check whether a port is host-facing
    # ---------------------------------------------------------

    def is_host_port(self, dpid, port_no):

        inter_switch_ports = set(
            self.adjacency.get(dpid, {}).values()
        )

        return (
            port_no not in inter_switch_ports
            and (dpid, port_no) not in self.down_ports
        )

    # ---------------------------------------------------------
    # 9. Calculate BFS spanning tree ports
    # ---------------------------------------------------------

    def get_spanning_tree_ports(self, dpid):

        tree_edges = set()
        visited = set()

        # Build a spanning forest, so disconnected
        # components can also be handled.
        for root in sorted(self.switches):

            if root in visited:
                continue

            visited.add(root)
            queue = [root]

            while queue:

                current = queue.pop(0)

                for neighbor in sorted(
                    self.adjacency.get(current, {})
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
    # 10. Find shortest path using BFS
    # ---------------------------------------------------------

    def get_shortest_path(self, src_dpid, dst_dpid):

        if src_dpid == dst_dpid:
            return [src_dpid]

        queue = [[src_dpid]]
        visited = {src_dpid}

        while queue:

            path = queue.pop(0)
            current = path[-1]

            if current == dst_dpid:
                return path

            for neighbor in sorted(
                self.adjacency.get(current, {})
            ):

                if neighbor not in visited:

                    visited.add(neighbor)

                    new_path = path + [neighbor]
                    queue.append(new_path)

        return []

    # ---------------------------------------------------------
    # 11. Determine output port
    # ---------------------------------------------------------

    def get_out_port(
        self,
        src_dpid,
        dst_dpid,
        dst_mac,
        in_port
    ):

        if src_dpid == dst_dpid:

            return self.mac_to_port.get(
                src_dpid, {}
            ).get(dst_mac)

        cache_key = (src_dpid, dst_dpid)

        if (
            self.mode == "static"
            and cache_key in self.static_paths_cache
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
    # 12. Main packet-in handler
    # ---------------------------------------------------------

    @set_ev_cls(
        ofp_event.EventOFPPacketIn,
        MAIN_DISPATCHER
    )
    def packet_in_handler(self, ev):

        msg = ev.msg

        datapath = msg.datapath
        dpid = datapath.id

        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        in_port = msg.match["in_port"]

        pkt = packet.Packet(msg.data)

        eth_list = pkt.get_protocols(ethernet.ethernet)

        if not eth_list:
            return

        eth = eth_list[0]

        # Ignore LLDP packets used for topology discovery.
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return

        src = eth.src
        dst = eth.dst

        # Drop packets from blocked IPv4 source addresses.
        ip_pkt = pkt.get_protocol(ipv4.ipv4)

        if ip_pkt and ip_pkt.src in self.blocked_ips:
            return

        # Learn MAC addresses only from host-facing ports.
        if self.is_host_port(dpid, in_port):

            self.mac_to_port.setdefault(dpid, {})

            if self.mac_to_port[dpid].get(src) != in_port:

                self.mac_to_port[dpid][src] = in_port

                self.install_class_rules(
                    datapath,
                    src,
                    in_port
                )

        # Find the switch where the destination MAC is known.
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

        if out_port is not None and out_port != in_port:

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

        else:

            # Unknown destination: flood only through
            # host-facing ports and spanning-tree ports.
            all_ports = [
                port
                for port in datapath.ports.keys()
                if port < ofproto.OFPP_MAX
            ]

            host_ports = [
                port
                for port in all_ports
                if self.is_host_port(dpid, port)
            ]

            tree_ports = list(
                self.get_spanning_tree_ports(dpid)
            )

            flood_ports = set(host_ports + tree_ports)

            # Never send the packet back through its input port.
            flood_ports.discard(in_port)

            actions = [
                parser.OFPActionOutput(port)
                for port in sorted(flood_ports)
            ]

        data = None

        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data

        out = parser.OFPPacketOut(
            datapath=datapath,
            buffer_id=msg.buffer_id,
            in_port=in_port,
            actions=actions,
            data=data
        )

        datapath.send_msg(out)

    # ---------------------------------------------------------
    # 13. Background flow monitoring
    # ---------------------------------------------------------

    def _monitor(self):

        while True:

            for datapath in list(
                self.datapaths.values()
            ):

                parser = datapath.ofproto_parser

                request = parser.OFPFlowStatsRequest(
                    datapath
                )

                datapath.send_msg(request)

            hub.sleep(MONITOR_INTERVAL)

    # ---------------------------------------------------------
    # 14. Process flow statistics
    # ---------------------------------------------------------

    @set_ev_cls(
        ofp_event.EventOFPFlowStatsReply,
        MAIN_DISPATCHER
    )
    def flow_stats_reply_handler(self, ev):

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
                    "CLASS=%s switch=%d %s=%d -> %s "
                    "packets=%d (+%d)",
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
# REST API CONTROLLER
# =============================================================

class PubSubRestController(ControllerBase):

    def __init__(
        self,
        req,
        resp,
        url,
        constraints,
        app
    ):

        super(PubSubRestController, self).__init__(
            req,
            resp,
            url,
            constraints,
            app
        )

        self.pubsub_app = app[PUBSUB_INSTANCE_NAME]

    # ---------------------------------------------------------
    # 15. Add a topic subscription
    # ---------------------------------------------------------

    @route(
        "pubsub",
        "/subscription",
        methods=["POST"]
    )
    def handle_subscription(self, req, **kwargs):

        try:

            body = json.loads(
                req.body.decode("utf-8")
            )

            topic = body.get("topic")
            ip = body.get("ip")

            if not topic or not ip:
                return json.dumps({
                    "status": "error",
                    "message": "Both topic and ip are required"
                })

            self.pubsub_app.subscriptions.setdefault(
                topic,
                []
            )

            if ip not in self.pubsub_app.subscriptions[topic]:
                self.pubsub_app.subscriptions[topic].append(ip)

            return json.dumps({
                "status": "success",
                "subscriptions": self.pubsub_app.subscriptions
            })

        except Exception as e:

            return json.dumps({
                "status": "error",
                "message": str(e)
            })

    # ---------------------------------------------------------
    # 16. Get all subscriptions
    # ---------------------------------------------------------

    @route(
        "pubsub",
        "/subscriptions",
        methods=["GET"]
    )
    def get_subscriptions(self, req, **kwargs):

        return json.dumps(
            self.pubsub_app.subscriptions
        )

    # ---------------------------------------------------------
    # 17. Select static or dynamic routing mode
    # ---------------------------------------------------------

    @route(
        "pubsub",
        "/mode",
        methods=["POST"]
    )
    def set_mode(self, req, **kwargs):

        try:

            body = json.loads(
                req.body.decode("utf-8")
            )

            new_mode = body.get("mode")

            if new_mode not in ("static", "dynamic"):

                return json.dumps({
                    "status": "error",
                    "message": "Use static or dynamic"
                })

            self.pubsub_app.mode = new_mode
            self.pubsub_app.static_paths_cache.clear()

            # Remove old forwarding entries so the new
            # routing mode is used for subsequent packets.
            self.pubsub_app.flush_all_switches()

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
    # 18. Get controller statistics
    # ---------------------------------------------------------

    @route(
        "pubsub",
        "/stats",
        methods=["GET"]
    )
    def get_stats(self, req, **kwargs):

        app = self.pubsub_app

        stats_data = {
            "mode": app.mode,
            "switches": list(app.datapaths.keys()),
            "adjacency": app.adjacency,
            "flow_counts": {
                str(key): value
                for key, value in app.last_counts.items()
            },
            "subscriptions": app.subscriptions,
            "blocked_ips": list(app.blocked_ips),
            "down_ports": list(app.down_ports)
        }

        return json.dumps(stats_data)

    # ---------------------------------------------------------
    # 19. Block an IPv4 source address
    # ---------------------------------------------------------

    @route(
        "pubsub",
        "/block",
        methods=["POST"]
    )
    def block_ip(self, req, **kwargs):

        try:

            body = json.loads(
                req.body.decode("utf-8")
            )

            ip_to_block = body.get("ip")

            if not ip_to_block:

                return json.dumps({
                    "status": "error",
                    "message": "No IP provided"
                })

            self.pubsub_app.blocked_ips.add(
                ip_to_block
            )

            # Remove old forwarding rules so subsequent
            # packets are checked by the controller.
            self.pubsub_app.flush_all_switches()

            return json.dumps({
                "status": "success",
                "blocked_ip": ip_to_block
            })

        except Exception as e:

            return json.dumps({
                "status": "error",
                "message": str(e)
            })
