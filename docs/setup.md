# Setup Guide

## 1. Install Mininet and tools
sudo apt update
sudo apt install mininet pipx python3-pip python3-venv -y

## 2. Install Ryu (Python 3.9 virtualenv, via uv)
pipx install uv
export PATH=$PATH:~/.local/bin
uv venv --python 3.9 ~/ryu-env
source ~/ryu-env/bin/activate
uv pip install setuptools==65.5.0 wheel pbr
PBR_VERSION=4.34 uv pip install --no-build-isolation eventlet==0.30.2 ryu
ryu-manager --version

## 3. Verify (use 2 terminals)
Terminal 1 (venv active, no sudo):
    sudo mn -c        <- run this FIRST, it kills any running Ryu
    source ~/ryu-env/bin/activate
    ryu-manager ryu.app.simple_switch_13
Terminal 2:
    sudo mn --controller=remote --topo single,3
    mininet> pingall      (expect 0% dropped; run twice if the first fails)

## 4. Startup order (every time)
1. sudo mn -c
2. Start Ryu
3. Start Mininet
Never run `mn -c` while Ryu is running.

## 5. Inspect flow rules (while Mininet is running)
sudo ovs-ofctl -O OpenFlow13 dump-flows s1
