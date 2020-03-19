# Tracker
Track every visitor click, setup growth experiments and measure every user outcome and growth loop all under one roof for all of your sites/assets without any external tools at unlimited scale (it's the same infrastructure that the big boys use: CERN, Netflix, Apple, Github). It's not exactly going to be a drop in replacement for Google Analytics, but it will go far beyond it to help you understand your users' experience. 

Don't want to give your user data to people you don't trust? Maybe save a GDPR lawsuit by using this. We've seen a marked drop in people sharing their data with Google Analytics, so this will allow you to get your own trusted statistics yourself.

## Features
* Tracking URL Generator extension for google chrome.
* Tracking API Calls & URLs & GET Redirects
* Tracking Images (for Emails)
* Reverse Proxy included (for your Node, Python, etc. API backend)
* TLS or LetsEncrypt one line configuration
* API & Request Rate Limiting
* Horizontally Scalable (Clustered NATS, Clustered Cassandra, Dockerized App Swarm - Good for ECS).
* File Server (w. Caching)
* Pluggable (Easily build more than Nats, Cassandra plugins)
* Server logging,counter and update messages built-in
* Works with REST & JSON out of the box
* Uncomplicated config.json one file configuration
* Initial tests show around 1,000 connections per second per server month dollar
* Written entirely in Golang
* Replaces much of Traefik's functionality
* Drop in replacement for InfluxData's Telegraf
* Drop in NGINX replacement 

## Compatible out of the box with
* Apache Spark
* Elastic Search
* Apache Superset (AirBnB)
* Cassandra
* Elassandra
* NATS.io
* Jupyter

![image](https://user-images.githubusercontent.com/760216/48519797-180ffb00-e823-11e8-9bae-ed21e169d6e2.png)


## Todo
* Kafka plugin
* NATS/Kafka converter/repeater
* Flink plugin
* Druid Plugin
* Apache SNS 
* Websocket Proxy (Ex. https://github.com/yhat/wsutil/blob/master/wsutil.go, https://gist.github.com/bradfitz/1d7bdf12278d4d713212ce6c74875dab) or wait for go 1.12

## Instructions

* Install Cassandra or Elassandra
* Install Schema to Cassandra https://github.com/dioptre/tracker/blob/master/.setup/schema.1.cql
* Insall Go > 1.9.3 (if you want to build from source)
* Get the tracker (if you want to build from source) ```go get github.com/dioptre/tracker && go build github.com/dioptre/tracker```
* Install Nats ```go get github.com/nats-io/gnatsd && go build github.com/nats-io/gnatsd```
* Go through the config.json file and change what you want.
* Deploy using Docker or ```go build```
* Use Spark, Kibana, etc to interrogate & ETL to your warehouse

Send the server something to track:
### REST Payload Example
In the following example, we use tuplets to persist what's needed to track (Ex. {"tr":"v1"})
```
https://localhost:8443/tr/v1/vid/14fb0860-b4bf-11e9-8971-7b80435315ac/ROCK/ON/lat/37.232332/lon/6.32233223/first/true/score/6/ref/14fb0860-b4bf-11e9-8971-7b80435315ac
```
### JSON Payload Example (Method:POST, Body)
Descriptions of the columns we send are in the schema file above. (Ex. vid = visitorId)
```json
{"last":"https://localhost:5001/cw.html","url":"https://localhost:5001/cw.html","params":{"type":"a","aff":"Bespoke"},"created":1539102052702,"duration":34752,"vid":"3d0be300-cbd2-11e8-aa59-ffd128a54d91","first":"false","sid":"3d0be301-cbd2-11e8-aa59-ffd128a54d91","tz":"America/Los_Angeles","device":"Linux","os":"Linux","sink":"cw$","score":1,"eid":"cw-a","uid":"admin"}
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

## Privacy
Since GDPR, honest reporting about user telemetry is required. The default tracker for online (https://github.com/dioptre/tracker/blob/master/.setup/www/track.js) uses a number of cookies by default:
* COOKIE_REFERRAL (ref): An entity that referred you to the site. 
* COOKIE_EXPERIMENT (xid): An experiment that you are in. A/B testing a button title for example.
* COOKIE_EXP_PARAMS (params): Additional information (experiment parameters) that stores information about you anonymously that can be used to tailor the experience to you.
* COOKIE_TRACK (trc): The last time you were tracked.
* COOKIE_VID (vid): Your unique id. This is consistent across all sessions, and is stored on your device.
* COOKIE_SESS (sess,sid): The session id. Each time you visit/use the site its approximately broken into session ids.
* COOKIE_JWT (jwt): The encrypted token of your user. This may optionally include your user id (uid) if logged in.


## Credits
* [DragonGate](https://github.com/dioptre/DragonGate)
* [SF Product Labs](https://sfproductlabs.com)

## Notes
* **This project is new** Please give me your feedback! We welcome PRs.
* This project is sort of the opposite to my horizontal web scraper in go https://github.com/dioptre/scrp

