#!/bin/bash
#./benchmark.mac.sh https://localhost:8443
wrk -t2 -c100 -d2s $1/v1/ppi/geoip?ip=8.8.8.8
