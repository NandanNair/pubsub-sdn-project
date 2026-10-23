#!/bin/sh
# usage: sh tests/pub.sh <topic> "<message>" [port]
(echo "$2"; sleep 1) | python3 clients/publisher.py 10.0.0.3 "$1" $3
