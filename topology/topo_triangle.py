#!/usr/bin/env python3
"""D2 topology: three switches in a triangle, two paths between publishers and the broker.

        pub1, pub2
            |
           s1 ----------- s3 --- broker
             \\           /
              \\         /
                  s2
                  |
              sub1, sub2

Paths from the publishers to the broker:
  primary : s1 - s3          (direct)
  backup  : s1 - s2 - s3     (used when the s1-s3 link fails)

The VM itself is attached to s3 with the address 10.0.0.254, so hosts can
reach the Ryu REST API at http://10.0.0.254:8080. (Inside Mininet, 127.0.0.1
on a host is the host's own loopback, not the controller.)

Usage:  sudo python3 topology/topo_triangle.py [--skip-ping] [--auto] [--bw MBIT]
  --bw 10   limit each switch-to-switch link to 10 Mbit/s (for the congestion experiment)
At the mininet> prompt:  link s1 s3 down   /   link s1 s3 up
"""
import sys
import time

from mininet.net import Mininet
from mininet.topo import Topo
from mininet.node import RemoteController, Node
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.log import setLogLevel, info

CONTROLLER_IP = "127.0.0.1"
CONTROLLER_PORT = 6653
ROOT_IP = "10.0.0.254/24"


class TriangleTopo(Topo):
    """Hosts keep the same IPs as D1. The broker is added first so that the
    Mininet MACs match D1 (:01 broker, :02 pub1, :03 pub2, :04 sub1, :05 sub2)."""

    def build(self, bw=None):
        # bw (Mbit/s) limits the three switch-to-switch links; None means unlimited.
        link_opts = {"bw": bw} if bw else {}
        s1 = self.addSwitch("s1", protocols="OpenFlow13")
        s2 = self.addSwitch("s2", protocols="OpenFlow13")
        s3 = self.addSwitch("s3", protocols="OpenFlow13")

        broker = self.addHost("broker", ip="10.0.0.3/24")
        pub1 = self.addHost("pub1", ip="10.0.0.1/24")
        pub2 = self.addHost("pub2", ip="10.0.0.2/24")
        sub1 = self.addHost("sub1", ip="10.0.0.4/24")
        sub2 = self.addHost("sub2", ip="10.0.0.5/24")

        # Host links.
        self.addLink(pub1, s1)
        self.addLink(pub2, s1)
        self.addLink(sub1, s2)
        self.addLink(sub2, s2)
        self.addLink(broker, s3)

        # Switch triangle.
        self.addLink(s1, s3, **link_opts)   # primary path
        self.addLink(s1, s2, **link_opts)   # backup path, first hop
        self.addLink(s2, s3, **link_opts)   # backup path, second hop


def link_down(net, a="s1", b="s3"):
    """Take a switch-to-switch link down (also used by the UI later)."""
    net.configLinkStatus(a, b, "down")


def link_up(net, a="s1", b="s3"):
    net.configLinkStatus(a, b, "up")


def build_network(bw=None):
    net = Mininet(
        topo=TriangleTopo(bw=bw),
        link=TCLink,
        controller=None,
        autoSetMacs=True,
        autoStaticArp=False,
    )
    net.addController(RemoteController("c0", ip=CONTROLLER_IP, port=CONTROLLER_PORT))

    # Attach the VM (root namespace) to s3 so hosts can reach the controller's REST API.
    root = Node("root", inNamespace=False)
    root_intf = net.addLink(root, net.get("s3")).intf1
    root.setIP(ROOT_IP, intf=root_intf)
    net.start()
    return net


def main():
    setLogLevel("info")
    bw = None
    if "--bw" in sys.argv:
        bw = float(sys.argv[sys.argv.index("--bw") + 1])
        info("*** Switch-to-switch links limited to %s Mbit/s\n" % bw)
    net = build_network(bw=bw)

    if "--skip-ping" not in sys.argv:
        info("*** Waiting for the controller to settle\n")
        time.sleep(3)
        net.pingAll()
    else:
        info("*** Skipping pingall (controller may not handle the loop yet)\n")

    if "--auto" in sys.argv:
        net.get("broker").cmd("python3 broker/broker.py > results/broker.log 2>&1 &")

    info("\n*** Try:\n")
    info("  link s1 s3 down        (fail the primary path)\n")
    info("  link s1 s3 up\n")
    info("  sudo ovs-ofctl -O OpenFlow13 dump-flows s1   (run in another terminal; also s2, s3)\n")
    info("  Controller REST address for hosts: http://10.0.0.254:8080\n")
    CLI(net)
    net.stop()


if __name__ == "__main__":
    main()
