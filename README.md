# Tracker
Track every visitor click, setup growth experiments and measure every user outcome all in house without any external tools at unlimited scale (it's the same infrastructure that the big boys use: CERN, Netflix, Apple, Github). It's not exactly going to be a drop in replacement for Google Analytics, but it will go far beyond it to help you understand your users' experience. 

Don't want to give your user data to people you don't trust? Maybe save a GDPR lawsuit by using this.

## Features
* Tracking API Calls & URLs
* Tracking Images (for Emails)
* Reverse Proxy included (one line Drop in NGINX replacement for your Node, Python, etc. API backend)
* LetsEncrypt one line configuration
* API & Request Rate Limiting
* Horizontally Scalable (Clustered NATS, Clustered Cassandra, Dockerized App Swarm - Good for ECS).
* File Server (w. Caching)
* Pluggable (Easily build more than Nats, Cassandra plugins)
* Server logging,counter and update messages built-in
* Works with REST & JSON out of the box
* Uncomplicated config.json one file configuration
* Initial tests show around 1,000 connections per second per server month dollar
* Written entirely in Golang

## Todo
* Kafka plugin
* NATS/Kafka converter/repeater
* Flink plugin
* Druid Plugin
* Growth Loop/s
* GET redirects

## Instructions

* Install Cassandra or Elassandra
* Install Schema to Cassandra https://github.com/dioptre/tracker/blob/master/.setup/schema.1.cql
* Install Nats
* Go through the config.json file and change what you want.
* Deploy using Docker or ```go build```
* Use Spark, Kibana, etc to interrogate & ETL to your warehouse

Send the server something to track:
### REST Payload Example
In the following example, we use tuplets to persist what's needed to track (Ex. {"tr":"v1"})
```
https://localhost:8443/tr/v1/vid/aFccafd/ROCK/ON/lat/37.232332/lon/6.32233223/first/true/score/6/ref/andy
```
### JSON Payload Example (Method:POST, Body)
Descriptions of the columns we send are in the schema file above. (Ex. vid = visitorId)
```json
{"last":"https://localhost:5001/cw.html","next":"https://localhost:5001/cw.html","params":{"type":"a","ref":"Bespoke"},"created":1539102052702,"duration":34752,"vid":"3d0be300-cbd2-11e8-aa59-ffd128a54d91","first":"false","sid":"3d0be301-cbd2-11e8-aa59-ffd128a54d91","tz":"America/Los_Angeles","device":"Linux","os":"Linux","sink":"cw$","score":1,"eid":"cw-a","uid":"admin"}
```

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

## Notes
* **This project is new** Please give me your feedback! We welcome PRs.
* This project is sort of the opposite to my horizontal web scraper in go https://github.com/dioptre/scrp

