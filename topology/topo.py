#!/usr/bin/env python3
"""
Mininet topology for the Pub-Sub + SDN project (D1).

Hosts:  pub1 10.0.0.1, pub2 10.0.0.2, broker 10.0.0.3, sub1 10.0.0.4, sub2 10.0.0.5
Switch: s1 (OpenFlow 1.3)
Controller: Ryu running separately on 127.0.0.1:6653

Run (Ryu must already be running):
    sudo python3 topology/topo.py            # opens the Mininet CLI
    sudo python3 topology/topo.py --auto     # also starts the broker automatically
"""

import argparse
import os

from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.topo import Topo

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BROKER_SCRIPT = os.path.join(REPO_ROOT, "broker", "broker.py")
LOG_DIR = os.path.join(REPO_ROOT, "results", "logs")


class PubSubTopo(Topo):
    """Single switch, five hosts."""

    def build(self):
        s1 = self.addSwitch("s1", protocols="OpenFlow13")

        pub1 = self.addHost("pub1", ip="10.0.0.1/24")
        pub2 = self.addHost("pub2", ip="10.0.0.2/24")
        broker = self.addHost("broker", ip="10.0.0.3/24")
        sub1 = self.addHost("sub1", ip="10.0.0.4/24")
        sub2 = self.addHost("sub2", ip="10.0.0.5/24")

        for h in (pub1, pub2, broker, sub1, sub2):
            self.addLink(h, s1)


def start_broker(net):
    """Start broker.py on the broker host in the background, logging to a file."""
    if not os.path.exists(BROKER_SCRIPT):
        info("*** broker/broker.py not found yet, skipping auto-start\n")
        return
    os.makedirs(LOG_DIR, exist_ok=True)
    broker = net.get("broker")
    broker.cmd("python3 %s > %s/broker.log 2>&1 &" % (BROKER_SCRIPT, LOG_DIR))
    info("*** Broker started on 10.0.0.3 (ports 5001, 5002), log: %s/broker.log\n" % LOG_DIR)


def run(auto):
    topo = PubSubTopo()
    net = Mininet(
        topo=topo,
        switch=OVSSwitch,
        controller=None,
        autoSetMacs=True,
    )
    net.addController("c0", controller=RemoteController, ip="127.0.0.1", port=6653)

    net.start()

    info("*** Testing basic connectivity\n")
    net.pingAll()

    if auto:
        start_broker(net)

    info("\n*** Useful commands inside the CLI:\n")
    info("    broker python3 broker/broker.py &         (start broker manually)\n")
    info("    sub1 python3 clients/subscriber.py --broker 10.0.0.3 --topics alerts\n")
    info("    pub1 python3 clients/publisher.py --broker 10.0.0.3 --topic alerts --count 5\n")
    info("    xterm sub1 pub1                            (open terminals on hosts)\n")
    info("    sh ovs-ofctl -O OpenFlow13 dump-flows s1   (show flow rules)\n\n")

    CLI(net)
    net.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--auto", action="store_true",
                        help="auto-start broker/broker.py on the broker host")
    args = parser.parse_args()
    setLogLevel("info")
    run(args.auto)
