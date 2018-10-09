# Tracker
Track every user click, setup growth experiments and measure every user outcome all in house without any external tools at unlimited scale (it's the same infrastructure that the big boys use: CERN, Netflix, Apple, Github). It's not going to be a drop in replacement for Google Analytics, but it will go far beyond it to help you understand your users experience.

## Features
* Tracking API Calls & URLs
* Tracking Images (for Emails)
* Proxy (NGINX replacement)
* LetsEncrypt one line configuration
* Rate Limiting
* Horizontally Scalable (Clustered NATS, Clustered Cassandra, Dockerized App Swarm - Good for ECS).
* File Server (w. Caching)
* Pluggable (Easily build more than Nats, Cassandra plugins)
* Server logging,counter and update messages built-in
* Works with REST & JSON out of the box
* Uncomplicated config.json one file configuration

## Instructions

* Install Cassandra or Elassandra
* Install Schema to Cassandra https://github.com/dioptre/tracker/blob/master/.setup/schema.1.cql
* Install Nats
* Go through the config.json file and change what you want.
* Use Spark, Kibana, etc to interrogate & ETL to your warehouse

### Deploy
* Get a certificate:
    * Deploy on AWS with config parameters in conjunction with KMS on ECS on Amazon AWS https://hackernoon.com/you-should-use-ssm-parameter-store-over-lambda-env-variables-5197fc6ea45b (see dockercmd.sh)
    * Use the above test key **must be server.crt and server.key**
    * Copy a server.crt and server.key in from sn SSL certificate provider.
* Update **config.json** to **UseLocalTLS=true** if required.
* Deploy on Docker using the following:
```
# Build from src:
sudo docker build -t tracker .
# Deploy only:
# sudo docker build -f Dockerfile.deploy -t tracker .
sudo docker run -p 8443:443 tracker
# Connect to it:
#  sudo docker ps
#  sudo docker exec -it [container_id] bash
# Remove all your images (warning):
#  sudo docker system prune -a
```
* Then upload/use (try AWS ECS).

## Credits
* [DragonGate](https://github.com/dioptre/DragonGate)
* [SF Product Labs](https://sfproductlabs.com)

