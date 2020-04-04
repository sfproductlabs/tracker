#!/bin/bash
cat ~/GH_TOKEN.txt | docker login docker.pkg.github.com -u dioptre --password-stdin
docker tag $(docker ps -lq) docker.pkg.github.com/sfproductlabs/tracker/tracker:latest
docker push docker.pkg.github.com/sfproductlabs/tracker/tracker:latest