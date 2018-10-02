#!/bin/bash
git submodule update --recursive --remote
cd ./tests/wrk
make
./wrk -t2 -c100 -d2s $1
cd ../..
