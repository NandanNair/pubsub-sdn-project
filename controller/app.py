"""
Ryu controller for the Pub-Sub + SDN project (D1 starter, v2).

What it does
------------
1. Learning switch (like ryu.app.simple_switch_13) so all hosts can talk.
2. PROACTIVE traffic classification by TCP port. As soon as a host's MAC is
   learned, the controller installs class rules towards that host:
       TCP port 5001 -> class HIGH   (priority 100)   e.g. topic "alerts"
       TCP port 5002 -> class NORMAL (priority 50)    every other topic
   Both directions are covered: tcp_dst (client -> broker) and
   tcp_src (broker -> client).
   These are installed proactively because the generic priority-1 L2 rules
   would otherwise match the TCP packets first and the controller would
   never see them.
3. Every 5 seconds the controller reads the flow counters and logs lines like
       CLASS=HIGH tcp_dst=5001 -> 00:00:00:00:00:03 packets=12 (+4)
   which is visible evidence for the demo (and a base for D2 monitoring).

Priorities
----------
    100  HIGH   (tcp port 5001)
     50  NORMAL (tcp port 5002)
      1  learned L2 forwarding
      0  table-miss -> send to controller

Run:   ryu-manager controller/app.py
Check: sudo ovs-ofctl -O OpenFlow13 dump-flows s1
"""

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.lib import hub
from ryu.lib.packet import ether_types, ethernet, packet
from ryu.ofproto import ofproto_v1_3

# tcp port -> (class name, flow priority)
TRAFFIC_CLASSES = {
    5001: ("HIGH", 100),
    5002: ("NORMAL", 50),
}

PRIORITY_L2 = 1
PRIORITY_TABLE_MISS = 0
MONITOR_INTERVAL = 5  # seconds


class PubSubController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(PubSubController, self).__init__(*args, **kwargs)
        self.mac_to_port = {}      # {dpid: {mac: port}}
        self.datapaths = {}        # {dpid: datapath}
        self.last_counts = {}      # {(dpid, priority, match): packet_count}
        self.monitor_thread = hub.spawn(self._monitor)

    # ------------------------------------------------------------------
    # Switch connects: install the table-miss rule
    # ------------------------------------------------------------------
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
        self.logger.info("Switch %s connected, table-miss rule installed",
                         datapath.id)

    # ------------------------------------------------------------------
    # Helper: install a flow entry
    # ------------------------------------------------------------------
    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
        if buffer_id is not None and buffer_id != ofproto.OFP_NO_BUFFER:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst)
        datapath.send_msg(mod)

    # ------------------------------------------------------------------
    # Install the TCP 5001/5002 class rules towards one host
    # ------------------------------------------------------------------
    def install_class_rules(self, datapath, mac, port):
        parser = datapath.ofproto_parser
        actions = [parser.OFPActionOutput(port)]
        for tcp_port, (cls, priority) in TRAFFIC_CLASSES.items():
            for field in ("tcp_dst", "tcp_src"):
                match = parser.OFPMatch(eth_type=ether_types.ETH_TYPE_IP,
                                        ip_proto=6, eth_dst=mac,
                                        **{field: tcp_port})
                self.add_flow(datapath, priority, match, actions)
        self.logger.info("Installed class rules (HIGH=5001, NORMAL=5002) "
                         "towards %s via port %d", mac, port)

    # ------------------------------------------------------------------
    # Packet-in: learn MACs, install class rules + L2 rule
    # ------------------------------------------------------------------
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match["in_port"]

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return  # ignore LLDP

        dst = eth.dst
        src = eth.src
        dpid = datapath.id

        # Learn the source MAC -> port. If it is new (or moved), install
        # the class rules towards it straight away.
        self.mac_to_port.setdefault(dpid, {})
        if self.mac_to_port[dpid].get(src) != in_port:
            self.mac_to_port[dpid][src] = in_port
            self.install_class_rules(datapath, src, in_port)

        out_port = self.mac_to_port[dpid].get(dst, ofproto.OFPP_FLOOD)
        actions = [parser.OFPActionOutput(out_port)]

        # Plain L2 rule (priority 1) once the destination is known
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst)
            if msg.buffer_id != ofproto.OFP_NO_BUFFER:
                self.add_flow(datapath, PRIORITY_L2, match, actions,
                              msg.buffer_id)
                return
            self.add_flow(datapath, PRIORITY_L2, match, actions)

        # Forward the packet that triggered this packet-in
        data = None
        if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            data = msg.data
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    # ------------------------------------------------------------------
    # Monitor: ask the switch for flow counters every few seconds
    # ------------------------------------------------------------------
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
                    "CLASS=%s %s=%d -> %s packets=%d (+%d)",
                    cls, field, tcp_port, stat.match.get("eth_dst", "?"),
                    stat.packet_count, stat.packet_count - previous)
            self.last_counts[key] = stat.packet_count
