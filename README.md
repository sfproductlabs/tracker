# Tracker
User telemetry. Currently in production use, capturing hundreds of millions of records.

Track every visitor click, setup growth experiments and measure every user outcome and growth loop all under one roof for all of your sites/assets without any external tools at unlimited scale (it's the same infrastructure that the big boys use: CERN, Netflix, Apple, Github). It's not exactly going to be a drop in replacement for Google Analytics, but it will go far beyond it to help you understand your users' experience. 

Don't want to give your user data to people you don't trust? Maybe save a GDPR lawsuit by using this. We've seen a marked drop in people sharing their data with Google Analytics, so this will allow you to get your own trusted statistics yourself. Solves problems with data sovereignty, data-residency and inter-continental privacy localization.

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
* GeoIP

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
* Install Schema to Cassandra https://github.com/dioptre/tracker/blob/master/.setup/schema.3.cql
* Insall Go > 1.9.3 (if you want to build from source)
* Get the tracker (if you want to build from source) ```go get github.com/dioptre/tracker && go build github.com/dioptre/tracker```
* You may need to update pebble to an older commit (b64dcf2173d7fa03f54db3df14b89876fa807e42) works.
* Install Nats ```go get github.com/nats-io/gnatsd && go build github.com/nats-io/gnatsd```
* Go through the config.json file and change what you want.
* Deploy using Docker or ```go build```
* Use Spark, Kibana, etc to interrogate & ETL to your warehouse

## API
### Track Request
Send the server something to track (replace tr with str if its from an internal service):

#### REST Payload Example
In the following example, we use tuplets to persist what's needed to track (Ex. {"tr":"v1"})
```
https://localhost:8443/tr/v1/vid/14fb0860-b4bf-11e9-8971-7b80435315ac/ROCK/ON/lat/37.232332/lon/6.32233223/first/true/score/6/ref/14fb0860-b4bf-11e9-8971-7b80435315ac
```
#### JSON Payload Example (Method:POST, Body)
Descriptions of the columns we send are in the schema file above. (Ex. vid = visitorId)
```json
{"last":"https://localhost:5001/cw.html","url":"https://localhost:5001/cw.html","params":{"type":"a","aff":"Bespoke"},"created":1539102052702,"duration":34752,"vid":"3d0be300-cbd2-11e8-aa59-ffd128a54d91","first":"false","sid":"3d0be301-cbd2-11e8-aa59-ffd128a54d91","tz":"America/Los_Angeles","device":"Linux","os":"Linux","sink":"cw$","score":1,"eid":"cw-a","uid":"admin"}
```
#### Failed Example
```
curl -k --header "Content-Type: application/json" \
  --request POST \
  --data '{"app":"native","email":"lalala@aaa.com","uid":"179ea090-6e8c-11ea-bb89-1d0ba023ecf8","uname":null,"tz":"Europe/Warsaw","device":"Handset","os":"iOS 13.4","did":"758152C1-278C-4C80-84A0-CF771B000835","w":375,"h":667,"rel":1,"sid":"c1dcf340-6eaa-11ea-a0b8-6120e9776df7","time":1585149028377,"ename":"filter_results","etyp":"filter","ptyp":"own_rooms","page":1,"vid":"016f2740-6e8c-11ea-9f0b-5d70c66851be"}' \
  https://localhost:443/tr/v1/ -vvv
```
#### Good Example
* Notice the additional param "page" needed to be a string
* Notice the "rel" application release also needed to be a string 
```
curl -k --header "Content-Type: application/json" \
  --request POST \
  --data '{"app":"native","email":"lalala@aaa.com","uid":"179ea090-6e8c-11ea-bb89-1d0ba023ecf8","uname":null,"tz":"Europe/Warsaw","device":"Handset","os":"iOS 13.4","did":"758152C1-278C-4C80-84A0-CF771B000835","w":375,"h":667,"rel":"1","sid":"c1dcf340-6eaa-11ea-a0b8-6120e9776df7","time":1585149028377,"ename":"filter_results","etyp":"filter","ptyp":"own_rooms","page":"1","vid":"016f2740-6e8c-11ea-9f0b-5d70c66851be"}' \
  https://localhost:443/tr/v1/ -vvv
```

### Shortened URLs

#### List Shortened URLs for a site
```
curl -k --request GET https://localhost:8443/rpi/v1/redirects/14fb0860-b4bf-11e9-8971-7b80435315ac/password/yoursitename.com
```
#### Create a Shortened URL
```
curl -k --request POST \
  --data '{"urlfrom":"https://yoursitename.com/test","hostfrom":"yoursrcsitename.com","slugfrom":"/test","urlto":"https://yoursitename.com/pathtourl?gu=1&ptyp=ad&utm_source=fb&utm_medium=content&utm_campaign=test_campaign&utm_content=clicked_ad&etype=user_click&ref=b7c551b2-857a-11ea-8eb7-de2e3c44e03d","hostto":"yourdestsitename.com","pathto":"/pathtourl","searchto":"?gu=1&ptyp=ad&utm_source=fb&utm_medium=content&utm_campaign=test_campaign&utm_content=clicked_ad&etype=user_click&ref=b7c551b2-857a-11ea-8eb7-de2e3c44e03d"}' \
  https://localhost:8443/rpi/v1/redirect/14fb0860-b4bf-11e9-8971-7b80435315ac/password/yoursitename.com
```


### Testing

Be extremely careful with schema. For performance, the _tracker_ takes client requests, and dumps the connection for speed. https://github.com/sfproductlabs/tracker/blob/0b205c5937ca6362ba7226b065e9750d79d107e0/.setup/schema.3.cql#L50

### Debugging
You can run a docker version of tracker using ```docker-compose up``` then ```./tracker``` after tracker is built. There is a setting in the ```config.json``` to enable debug tracing on the command line. It will print any errors to the console of the running service. These are not saved, or distributed to any log for performance reasons. So test test test.


### Deploy

#### Docker
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

#### Debian
```sh
mkdir tracker
cd tracker/
git clone https://github.com/sfproductlabs/tracker .
sudo apt update
sudo apt install curl
cd ..
curl -O https://dl.google.com/go/go1.12.7.linux-amd64.tar.gz
sha256sum go1.12.7.linux-amd64.tar.gz
#66d83bfb5a9ede
tar xvf go1.12.7.linux-amd64.tar.gz
sudo chown -R root:root ./go
sudo mv go /usr/local
echo "export GOPATH=$HOME/gocode" >> ~/.bashrc
echo "export PATH=$PATH:/usr/local/go/bin:$GOPATH/bin" >> ~/.bashrc
# vi .bashrc 
source ~/.bashrc 
go version
cd tracker/
go build
go get github.com/sfproductlabs/tracker && go build github.com/sfproductlabs/tracker
go build
cd ~/gocode/src/github.com/cockroachdb/pebble
git checkout b64dcf2173d7fa03f54db3df14b89876fa807e42
git checkout b64dcf2
go build
cd ~/tracker/
go build
```

## Privacy
Since GDPR, honest reporting about user telemetry is required. The default tracker for online (https://github.com/dioptre/tracker/blob/master/.setup/www/track.js) uses a number of cookies by default:
* COOKIE_REFERRAL (ref): An entity that referred you to the site. 
* COOKIE_EXPERIMENT (xid): An experiment that you are in. A/B testing a button title for example.
* COOKIE_EXP_PARAMS (params): Additional information (experiment parameters) that stores information about you anonymously that can be used to tailor the experience to you.
* COOKIE_TRACK (trc): The last time you were tracked.
* COOKIE_VID (vid): Your unique id. This is consistent across all sessions, and is stored on your device.
* COOKIE_SESS (sess,sid): The session id. Each time you visit/use the site its approximately broken into session ids.
* COOKIE_JWT (jwt): The encrypted token of your user. This may optionally include your user id (uid) if logged in.

### Pruning Records
* Run  ```./tracker --prune config.json``` to run privacy pruning.
## Credits
* [DragonGate](https://github.com/dioptre/DragonGate)
* [SF Product Labs](https://sfproductlabs.com)
* This site or product includes IP2Location LITE data available from https://lite.ip2location.com.

## Notes
* **This project is in production** and has seen significant improvements in revenue for its users.
* This project is sort of the opposite to my horizontal web scraper in go https://github.com/dioptre/scrp

## Testing

### Testing within ECS docker container

* Make sure Debug in config.json is set to **true**
* Try running in an ecs instance (`ssh -l ec2-user 172.18.99.1; docker ps; docker exec -it aaa bash;`):
```bash
apt install curl procps vim

#Find the process
ps waux | grep tracker
#Kill the old tracker process with kill
#kill 70
#Replace "Debug" : true (in config.json)
#Run . /tracker/tracker config.json
#Do this QUICKLY before the machine is swapped out due to excessive downtime 

#Run your test in another terminal... ssh -l ec2-user 172.18.99.1 (from ecs service) and docker exec -it aa bash
curl -w "\n" -k -H 'Content-Type: application/json'  -XPOST  "https://localhost:8443/tr/v1/" -d '{"hideFreePlan":"false","name":"Bewusstsein in Aufruhr","newsletter":"bewusstsein-in-aufruhr","static":"%2Fkurs%2Fbewusstsein-in-aufruhr","umleitung":"%2Fkurs%2Fbewusstsein-in-aufruhr","ename":"visited_site","etyp":"session","last":"/einloggen","url":"/registrieren","ptyp":"logged_out_ancillary","sid":"627f7c80-0d7c-11eb-9767-93f1d9c02a9c","first":"true","tz":"America/Los_Angeles","device":"Mac","os":"macOS","w":1331,"h":459,"vid":"627f7c80-0d7c-11eb-9767-93f1d9c02a9c","rel":"1.0.179","app":"hd","params":{"hideFreePlan":"false","name":"Bewusstsein in Aufruhr","newsletter":"bewusstsein-in-aufruhr","static":"%2Fkurs%2Fbewusstsein-in-aufruhr","umleitung":"%2Fkurs%2Fbewusstsein-in-aufruhr","ename":"viewed_page","etyp":"view","last":"/einloggen","url":"/registrieren","ptyp":"logged_out_ancillary","sid":"627f7c80-0d7c-11eb-9767-93f1d9c02a9c","first":"true","tz":"America/Los_Angeles","device":"Mac","os":"macOS","w":1331,"h":459,"vid":"627f7c80-0d7c-11eb-9767-93f1d9c02a9c","rel":"1.0.179","app":"hd","homepageSlogan":"B","homepagePricePlans":"A"}}'

#or check tlv
curl -w "\n" -k -H 'Content-Type: application/json'  -XPOST  "https://localhost:8443/tlv/v1/" -d '{"vid":"627f7c80-0d7c-11eb-9767-93f1d9c02a9c","uid":"627f7c80-0d7c-11eb-9767-93f1d9c02a9c","sid":"627f7c80-0d7c-11eb-9767-93f1d9c02a9c", "orid":"627f7c80-0d7c-11eb-9767-93f1d9c02a9c", "amt" : 35}'

#or privacy
curl -w "\n" -k -H 'Content-Type: application/json' -XPOST  "https://localhost:8443/ppi/v1/agree" -d '{"vid": "5ae3c890-5e55-11ea-9283-4fa18a847130", "cflags": 1024}'
```
