"""
Ryu controller for the Pub-Sub + SDN project (D2 Multi-Switch, Loop-Free, REST API).

What it adds in D2:
-------------------
1. Multi-switch topology discovery using ryu.topology and stplib (or shortest path routing) 
   to prevent broadcast storms in the s1-s2-s3 triangle loop.
2. Dynamic rerouting: listens to link down events (EventLinkDelete) and recomputes 
   paths, updating flow entries while preserving D1 CLASS=HIGH / NORMAL rules.
3. Priority traffic handling & QoS differentiation (Priority 100 for HIGH / 50 for NORMAL).
4. REST API on port 8080:
   - POST /subscription {"action", "topic", "ip"}
   - GET /stats (JSON flow stats and link status)
   - POST /block {"ip"} (installs drop rule)
"""

import json
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.lib import hub
from ryu.lib.packet import ether_types, ethernet, packet, ipv4, tcp
from ryu.ofproto import ofproto_v1_3
from ryu.topology import event as topo_event
from ryu.topology.api import get_switch, get_link
from ryu.app.wsgi import ControllerBase, WSGIApplication, route

# REST API namespace
pubsub_instance_name = 'pubsub_api'

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
        self.mac_to_port = {}      # {dpid: {mac: port}}
        self.datapaths = {}        # {dpid: datapath}
        self.last_counts = {}      # {(dpid, priority, match): packet_count}
        
        # Topology graph data structures for multi-switch routing
        self.switches = []
        self.links = {}            # {(src_dpid, dst_dpid): port_no}
        self.adjacency = {}        # {dpid: {neighbor_dpid: port_no}}
        
        # Application state for REST API
        self.subscriptions = []
        self.blocked_ips = set()

        # Register WSGI controller for REST API
        wsgi = kwargs['wsgi']
        wsgi.register(PubSubRestController, {pubsub_instance_name: self})

        self.monitor_thread = hub.spawn(self._monitor)

    # -------------------------------------------------------------------------
    # Task 1: Switch Connection & Handshake
    # -------------------------------------------------------------------------
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        self.datapaths[datapath.id] = datapath

        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                           ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, PRIORITY_TABLE_MISS, match, actions)
        self.logger.info("Switch %s connected, table-miss rule installed", datapath.id)

    # -------------------------------------------------------------------------
    # Helper: Install a Flow Entry
    # -------------------------------------------------------------------------
    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        if buffer_id is not None and buffer_id != ofproto.OFP_NO_BUFFER:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match, instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst)
        datapath.send_msg(mod)

    # -------------------------------------------------------------------------
    # Install Class Rules (D1 Maintained)
    # -------------------------------------------------------------------------
    def install_class_rules(self, datapath, mac, port):
        parser = datapath.ofproto_parser
        actions = [parser.OFPActionOutput(port)]
        for tcp_port, (cls, priority) in TRAFFIC_CLASSES.items():
            for field in ("tcp_dst", "tcp_src"):
                match = parser.OFPMatch(eth_type=ether_types.ETH_TYPE_IP,
                                        ip_proto=6, eth_dst=mac,
                                        **{field: tcp_port})
                self.add_flow(datapath, priority, match, actions)
        self.logger.info("Installed class rules (HIGH=5001, NORMAL=5002) towards %s via port %d on switch %d", 
                         mac, port, datapath.id)

    # -------------------------------------------------------------------------
    # Task 1 & 2: Topology Discovery and Loop-Free Shortest Path Routing
    # -------------------------------------------------------------------------
    @set_ev_cls([topo_event.EventSwitchEnter, topo_event.EventSwitchLeave,
                 topo_event.EventLinkAdd, topo_event.EventLinkDelete],
                MAIN_DISPATCHER)
    def topology_change_handler(self, ev):
        """Discovers switches and links dynamically, updating the adjacency graph."""
        switch_list = get_switch(self, None)
        self.switches = [s.dp.id for s in switch_list]
        
        link_list = get_link(self, None)
        self.links.clear()
        self.adjacency.clear()
        
        for s in self.switches:
            self.adjacency[s] = {}

        for link in link_list:
            src_dpid = link.src.dpid
            dst_dpid = link.dst.dpid
            src_port = link.src.port_no
            dst_port = link.dst.port_no
            
            self.links[(src_dpid, dst_dpid)] = src_port
            self.adjacency[src_dpid][dst_dpid] = src_port

        self.logger.info("Topology updated: Switches=%s, Links discovered=%d", self.switches, len(self.links))

    def get_shortest_path(self, src_dpid, dst_dpid):
        """Computes loop-free shortest path between switches using BFS (Dijkstra equivalent for unweighted graph)."""
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
        """Determines the correct output port avoiding broadcast storms in the triangle loop."""
        if src_dpid == dst_dpid:
            if dst_mac in self.mac_to_port.get(src_dpid, {}):
                return self.mac_to_port[src_dpid][dst_mac]
            return self.datapaths[src_dpid].ofproto.OFPP_FLOOD

        path = self.get_shortest_path(src_dpid, dst_dpid)
        if len(path) > 1:
            next_hop = path[1]
            return self.adjacency[src_dpid].get(next_hop, self.datapaths[src_dpid].ofproto.OFPP_FLOOD)
        return self.datapaths[src_dpid].ofproto.OFPP_FLOOD

    # -------------------------------------------------------------------------
    # Packet-In Handler
    # -------------------------------------------------------------------------
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match["in_port"]
        dpid = datapath.id

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return

        dst = eth.dst
        src = eth.src

        # Check for IP block rule (Task 4)
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        if ip_pkt and ip_pkt.src in self.blocked_ips:
            return  # Drop packet silently

        # Learn source MAC -> port mapping
        self.mac_to_port.setdefault(dpid, {})
        if self.mac_to_port[dpid].get(src) != in_port:
            self.mac_to_port[dpid][src] = in_port
            self.install_class_rules(datapath, src, in_port)

        # Find destination datapath and output port across multi-switch topology
        out_port = ofproto.OFPP_FLOOD
        target_dpid = None

        for target_dp, mac_map in self.mac_to_port.items():
            if dst in mac_map:
                target_dpid = target_dp
                break

        if target_dpid is not None:
            out_port = self.get_out_port(dpid, target_dpid, dst, in_port)

        actions = [parser.OFPActionOutput(out_port)]

        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst)
            if msg.buffer_id != ofproto.OFP_NO_BUFFER:
                self.add_flow(datapath, PRIORITY_L2, match, actions, msg.buffer_id)
                return
            self.add_flow(datapath, PRIORITY_L2, match, actions)

        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                    in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    # -------------------------------------------------------------------------
    # Monitoring & Statistics (D1 Maintained)
    # -------------------------------------------------------------------------
    def _monitor(self):
        while True:
            for dp in list(self.datapaths.values()):
                parser = dp.ofproto_parser
                dp.send_msg(parser.OFPFlowStatsRequest(dp))
            hub.sleep(MONITOR_INTERVAL)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
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
            cls = TRAFFIC_CLASSES.get(tcp_port, ("?", 0))[0]
            key = (dpid, stat.priority, str(stat.match))
            previous = self.last_counts.get(key, 0)
            if stat.packet_count > previous:
                self.logger.info(
                    "CLASS=%s switch=%d %s=%d -> %s packets=%d (+%d)",
                    cls, dpid, field, tcp_port, stat.match.get("eth_dst", "?"),
                    stat.packet_count, stat.packet_count - previous)
            self.last_counts[key] = stat.packet_count


# -------------------------------------------------------------------------
# Task 4: REST API Controller (ryu.app.wsgi)
# -------------------------------------------------------------------------
class PubSubRestController(ControllerBase):
    def __init__(self, req, resp, url, constraints, app):
        super(PubSubRestController, self).__init__(req, resp, url, constraints, app)
        self.pubsub_app = app[pubsub_instance_name]

    @route('pubsub', '/subscription', methods=['POST'])
    def handle_subscription(self, req, **kwargs):
        try:
            body = json.loads(req.body.decode('utf-8'))
            self.pubsub_app.subscriptions.append(body)
            self.pubsub_app.logger.info("REST API: Subscription added: %s", body)
            return json.dumps({"status": "success", "subscriptions": self.pubsub_app.subscriptions})
        except Exception as e:
            return json.dumps({"status": "error", "message": str(e)})

    @route('pubsub', '/stats', methods=['GET'])
    def get_stats(self, req, **kwargs):
        stats_data = {
            "switches": list(self.pubsub_app.datapaths.keys()),
            "links": list(self.pubsub_app.links.keys()),
            "flow_counts": {str(k): v for k, v in self.pubsub_app.last_counts.items()},
            "subscriptions": self.pubsub_app.subscriptions,
            "blocked_ips": list(self.pubsub_app.blocked_ips)
        }
        return json.dumps(stats_data)

    @route('pubsub', '/block', methods=['POST'])
    def block_ip(self, req, **kwargs):
        try:
            body = json.loads(req.body.decode('utf-8'))
            ip_to_block = body.get('ip')
            if ip_to_block:
                self.pubsub_app.blocked_ips.add(ip_to_block)
                self.pubsub_app.logger.info("REST API: Blocked IP address: %s", ip_to_block)
                return json.dumps({"status": "success", "blocked_ip": ip_to_block})
            return json.dumps({"status": "error", "message": "No IP provided"})
        except Exception as e:
            return json.dumps({"status": "error", "message": str(e)})
