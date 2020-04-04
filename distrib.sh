#!/bin/bash
cat ~/.GH_TOKEN | sudo docker login docker.pkg.github.com -u dioptre --password-stdin
sudo docker tag $(docker ps -lq) docker.pkg.github.com/sfproductlabs/tracker/tracker:latest
sudo docker push docker.pkg.github.com/sfproductlabs/tracker/tracker:latest