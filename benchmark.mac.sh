#!/bin/bash
#./benchmark.mac.sh https://localhost:8443
wrk -t2 -c100 -d2s $1/ppi/v1/geoip?ip=8.8.8.8
