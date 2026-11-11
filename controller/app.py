"""
Ryu OpenFlow 1.3 Controller for Pub-Sub SDN Project (Deliverable 2).

Viva Explanation & Architecture:
- Loop Prevention: Replaces OFPP_FLOOD with a BFS Spanning Tree over the triangle topology.
- MAC Learning: Learns host MACs strictly from host-facing ports.
- Instant Rerouting: Listens to EventOFPPortStatus and EventLinkDelete to instantly update adjacency and flush stale flows.
- QoS & Modes: Supports static/dynamic modes and TCP port QoS classification (5001 HIGH, 5002 NORMAL).
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
        
        # Topology state
        self.switches = []
        self.links = {}            # {(src_dpid, dst_dpid): port_no}
        self.adjacency = {}        # {dpid: {neighbor_dpid: port_no}}
        
        # App state & REST config
        self.subscriptions = {}    # {topic: [ip1, ip2, ...]}
        self.blocked_ips = set()
        self.mode = "dynamic"      # "static" or "dynamic"
        self.static_paths_cache = {}

        wsgi = kwargs['wsgi']
        wsgi.register(PubSubRestController, {pubsub_instance_name: self})

        self.monitor_thread = hub.spawn(self._monitor)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        """Initializes switch datapath and installs priority-0 table-miss flow rule."""
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        self.datapaths[datapath.id] = datapath

        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                           ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, PRIORITY_TABLE_MISS, match, actions)
        self.logger.info("Switch %s connected, table-miss rule installed", datapath.id)

    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        """Helper function to install OpenFlow flow entries on a datapath."""
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

    def install_class_rules(self, datapath, mac, port):
        """Installs proactive QoS class rules (TCP 5001 HIGH, TCP 5002 NORMAL) towards a host."""
        if self.mode == "static":
            return
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

    @set_ev_cls([topo_event.EventSwitchEnter, topo_event.EventSwitchLeave,
                 topo_event.EventLinkAdd, topo_event.EventLinkDelete],
                MAIN_DISPATCHER)
    def topology_change_handler(self, ev):
        """Discovers topology links/switches, recalculates adjacency, and flushes stale flows."""
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
            
            self.links[(src_dpid, dst_dpid)] = src_port
            self.adjacency[src_dpid][dst_dpid] = src_port

        self.static_paths_cache.clear()
        self.logger.info("Topology updated: Switches=%s, Links discovered=%d", self.switches, len(self.links))

        if isinstance(ev, topo_event.EventLinkDelete):
            self.flush_all_switches()

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def port_status_handler(self, ev):
        """Listens to physical port state changes for instant link-failure detection."""
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        dpid = datapath.id
        port_no = msg.desc.port_no

        # Check if link/port went down
        if msg.desc.state & ofproto.OFPPS_LINK_DOWN or (msg.desc.config & ofproto.OFPPC_PORT_DOWN):
            changed = False
            for src_dpid, nbrs in list(self.adjacency.items()):
                for dst_dpid, p in list(nbrs.items()):
                    if src_dpid == dpid and p == port_no:
                        del self.adjacency[src_dpid][dst_dpid]
                        changed = True
            if changed:
                self.static_paths_cache.clear()
                self.logger.info("Instant Port Down detected on switch %d port %d. Flushing flows.", dpid, port_no)
                self.flush_all_switches()

    def flush_all_switches(self):
        """Flushes priority >= 1 flow rules across all connected switches to force re-routing."""
        for datapath in self.datapaths.values():
            ofproto = datapath.ofproto
            parser = datapath.ofproto_parser
            match = parser.OFPMatch()
            mod = parser.OFPFlowMod(
                datapath=datapath,
                command=ofproto.OFPFC_DELETE,
                out_port=ofproto.OFPP_ANY,
                out_group=ofproto.OFPG_ANY,
                priority=1,
                match=match
            )
            datapath.send_msg(mod)

    def is_host_port(self, dpid, port_no):
        """Determines if a port is host-facing (not connected to another switch)."""
        inter_switch_ports = set(self.adjacency.get(dpid, {}).values())
        return port_no not in inter_switch_ports

    def get_spanning_tree_ports(self, dpid):
        """Computes a loop-free BFS spanning tree from switch adjacency to prevent broadcast storms."""
        if not self.switches:
            return set()
        root = min(self.switches)
        tree_edges = set()
        visited = {root}
        queue = [root]
        
        while queue:
            curr = queue.pop(0)
            for nbr in sorted(self.adjacency.get(curr, {}).keys()):
                if nbr not in visited:
                    visited.add(nbr)
                    tree_edges.add((curr, nbr))
                    tree_edges.add((nbr, curr))
                    queue.append(nbr)
        
        tree_ports = set()
        for nbr, port in self.adjacency.get(dpid, {}).items():
            if (dpid, nbr) in tree_edges:
                tree_ports.add(port)
        return tree_ports

    def get_shortest_path(self, src_dpid, dst_dpid):
        """Computes shortest path between switches using BFS."""
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
        """Determines output port based on static or dynamic mode shortest path."""
        if src_dpid == dst_dpid:
            if dst_mac in self.mac_to_port.get(src_dpid, {}):
                return self.mac_to_port[src_dpid][dst_mac]
            return None

        cache_key = (src_dpid, dst_dpid)
        if self.mode == "static" and cache_key in self.static_paths_cache:
            path = self.static_paths_cache[cache_key]
        else:
            path = self.get_shortest_path(src_dpid, dst_dpid)
            if self.mode == "static":
                self.static_paths_cache[cache_key] = path

        if len(path) > 1:
            next_hop = path[1]
            return self.adjacency[src_dpid].get(next_hop)
        return None

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        """Handles packet-ins, enforcing host-only MAC learning, loop-free flooding, and routing."""
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

        # Drop blocked IPs
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        if ip_pkt and ip_pkt.src in self.blocked_ips:
            return

        # Learn host MACs strictly from host-facing ports
        if self.is_host_port(dpid, in_port):
            self.mac_to_port.setdefault(dpid, {})
            if self.mac_to_port[dpid].get(src) != in_port:
                self.mac_to_port[dpid][src] = in_port
                self.install_class_rules(datapath, src, in_port)

        # Find destination switch and output port
        out_port = None
        target_dpid = None

        for target_dp, mac_map in self.mac_to_port.items():
            if dst in mac_map:
                target_dpid = target_dp
                break

        if target_dpid is not None:
            out_port = self.get_out_port(dpid, target_dpid, dst, in_port)

        # Loop-free flooding using dynamic datapath ports + spanning tree ports
        if out_port is None:
            all_ports = [p for p in datapath.ports.keys() if p < ofproto.OFPP_MAX]
            host_facing = [p for p in all_ports if self.is_host_port(dpid, p)]
            st_ports = list(self.get_spanning_tree_ports(dpid))
            
            flood_ports = set(host_facing + st_ports)
            if in_port in flood_ports:
                flood_ports.remove(in_port)
            
            actions = [parser.OFPActionOutput(p) for p in flood_ports]
        else:
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

    def _monitor(self):
        """Periodic background task auditing flow statistics."""
        while True:
            for dp in list(self.datapaths.values()):
                parser = dp.ofproto_parser
                dp.send_msg(parser.OFPFlowStatsRequest(dp))
            hub.sleep(MONITOR_INTERVAL)

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def flow_stats_reply_handler(self, ev):
        """Processes flow statistics replies and logs classification events."""
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


class PubSubRestController(ControllerBase):
    """REST API Controller implementing subscription, stats, blocking, and mode selection endpoints."""
    def __init__(self, req, resp, url, constraints, app):
        super(PubSubRestController, self).__init__(req, resp, url, constraints, app)
        self.pubsub_app = app[pubsub_instance_name]

    @route('pubsub', '/subscription', methods=['POST'])
    def handle_subscription(self, req, **kwargs):
        try:
            body = json.loads(req.body.decode('utf-8'))
            topic = body.get('topic')
            ip = body.get('ip')
            if topic and ip:
                self.pubsub_app.subscriptions.setdefault(topic, [])
                if ip not in self.pubsub_app.subscriptions[topic]:
                    self.pubsub_app.subscriptions[topic].append(ip)
            self.pubsub_app.logger.info("REST API: Subscription added: %s", body)
            return json.dumps({"status": "success", "subscriptions": self.pubsub_app.subscriptions})
        except Exception as e:
            return json.dumps({"status": "error", "message": str(e)})

    @route('pubsub', '/subscriptions', methods=['GET'])
    def get_subscriptions(self, req, **kwargs):
        return json.dumps(self.pubsub_app.subscriptions)

    @route('pubsub', '/mode', methods=['POST'])
    def set_mode(self, req, **kwargs):
        try:
            body = json.loads(req.body.decode('utf-8'))
            new_mode = body.get('mode')
            if new_mode in ("static", "dynamic"):
                self.pubsub_app.mode = new_mode
                self.pubsub_app.static_paths_cache.clear()
                self.pubsub_app.logger.info("REST API: Controller mode set to %s", new_mode)
                return json.dumps({"status": "success", "mode": self.pubsub_app.mode})
            return json.dumps({"status": "error", "message": "Invalid mode. Use 'static' or 'dynamic'."})
        except Exception as e:
            return json.dumps({"status": "error", "message": str(e)})

    @route('pubsub', '/stats', methods=['GET'])
    def get_stats(self, req, **kwargs):
        stats_data = {
            "mode": self.pubsub_app.mode,
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
