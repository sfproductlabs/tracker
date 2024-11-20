#!/bin/bash
git submodule update --init --recursive
git submodule update --recursive --remote
cd ./tests/wrk
make
./wrk -t2 -c100 -d2s $1/v1/tr/vid/aFccafd/ROCK/ON/lat/37.232332/lon/6.32233223/first/false/score/6/ref/andy
./wrk -t2 -c100 -d2s $1/v1/ppi/geoip?ip=8.8.8.8
cd ../..
