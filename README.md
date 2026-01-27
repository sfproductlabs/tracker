# Tracker

## TLDR

```bash
git clone https://github.com/sfproductlabs/tracker.git
cd tracker
go get github.com/sfproductlabs/tracker && go build -o tracker
./tracker config.json
```

## Description
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
* Clickhouse
* DuckDB

![image](https://user-images.githubusercontent.com/760216/48519797-180ffb00-e823-11e8-9bae-ed21e169d6e2.png)


## Todo
* Apache Pulsar
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
https://localhost:8443/tr/v1/tr/vid/14fb0860-b4bf-11e9-8971-7b80435315ac/ROCK/ON/lat/37.232332/lon/6.32233223/first/true/score/6/ref/14fb0860-b4bf-11e9-8971-7b80435315ac
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
  https://localhost:443/tr/v1/tr/ -vvv
```
#### Good Example
* Notice the additional param "page" needed to be a string
* Notice the "rel" application release also needed to be a string 
```
curl -k --header "Content-Type: application/json" \
  --request POST \
  --data '{"app":"native","email":"lalala@aaa.com","uid":"179ea090-6e8c-11ea-bb89-1d0ba023ecf8","uname":null,"tz":"Europe/Warsaw","device":"Handset","os":"iOS 13.4","did":"758152C1-278C-4C80-84A0-CF771B000835","w":375,"h":667,"rel":"1","sid":"c1dcf340-6eaa-11ea-a0b8-6120e9776df7","time":1585149028377,"ename":"filter_results","etyp":"filter","ptyp":"own_rooms","page":"1","vid":"016f2740-6e8c-11ea-9f0b-5d70c66851be"}' \
  https://localhost:443/tr/v1/tr/ -vvv
```

### Shortened URLs

#### List Shortened URLs for a site
```
curl -k --request GET https://localhost:8443/tr/v1/rpi/redirects/14fb0860-b4bf-11e9-8971-7b80435315ac/password/yoursitename.com
```
#### Create a Shortened URL
```
curl -k --request POST \
  --data '{"urlfrom":"https://yoursitename.com/test","hostfrom":"yoursrcsitename.com","slugfrom":"/test","urlto":"https://yoursitename.com/pathtourl?gu=1&ptyp=ad&utm_source=fb&utm_medium=content&utm_campaign=test_campaign&utm_content=clicked_ad&etype=user_click&ref=b7c551b2-857a-11ea-8eb7-de2e3c44e03d","hostto":"yourdestsitename.com","pathto":"/pathtourl","searchto":"?gu=1&ptyp=ad&utm_source=fb&utm_medium=content&utm_campaign=test_campaign&utm_content=clicked_ad&etype=user_click&ref=b7c551b2-857a-11ea-8eb7-de2e3c44e03d"}' \
  https://localhost:8443/tr/v1/rpi/redirect/14fb0860-b4bf-11e9-8971-7b80435315ac/password/yoursitename.com
```


### Testing

Be extremely careful with schema. For performance, the _tracker_ takes client requests, and dumps the connection for speed. https://github.com/sfproductlabs/tracker/blob/0b205c5937ca6362ba7226b065e9750d79d107e0/.setup/schema.3.cql#L50

### Debugging
You can run a docker version of tracker using ```docker-compose up``` then ```./tracker``` after tracker is built. There is a setting in the ```config.json``` to enable debug tracing on the command line. It will print any errors to the console of the running service. These are not saved, or distributed to any log for performance reasons. So test test test.


### Makefile Commands (Recommended)

The tracker includes a comprehensive Makefile for streamlined development and testing workflows. Use `make help` to see all available commands.

#### Quick Start with Makefile

```bash
# Build and test everything
make docker-build         # Build Docker image
make docker-run           # Start single-node container
make docker-test-all      # Run comprehensive tests

# Or do it all in one command
make docker-rebuild-test  # Clean rebuild + full test suite
```

#### Available Makefile Targets

**Build Commands:**
```bash
make build                # Build tracker binary locally
make run                  # Build and run tracker (local mode)
make clean                # Clean build artifacts
make deps                 # Download Go dependencies
make fmt                  # Format Go code
make lint                 # Run golangci-lint
```

**Docker Commands (Single Node):**
```bash
make docker-build         # Build Docker image
make docker-run           # Run single-node container with persistent volumes
make docker-stop          # Stop and remove container
make docker-clean         # Remove container, image, and volumes
make docker-logs          # Show container logs (tail -f)
make docker-shell         # Open shell in running container
make docker-clickhouse-shell  # Open ClickHouse client in container
make docker-verify-tables     # Verify ClickHouse tables loaded (should show 236 tables)
```

**Docker Testing Commands:**
```bash
make docker-test-events   # Test events table with sample data
make docker-test-messaging # Test messaging tables (mthreads/mstore/mtriage)
make docker-test-all      # Run all Docker tests
make docker-rebuild-test  # Clean rebuild and full test
```

**Functional Endpoint Tests:**
```bash
make test-functional-health       # Test /health, /ping, /status, /metrics
make test-functional-ltv          # Test LTV tracking (single payment)
make test-functional-ltv-batch    # Test LTV tracking (batch payments)
make test-functional-redirects    # Test redirect/short URL API
make test-functional-privacy      # Test privacy/agreement API
make test-functional-jurisdictions # Test jurisdictions endpoint
make test-functional-batch        # Test batch processing (100 events)
make test-functional-e2e          # Test complete end-to-end workflow
make test-functional-all          # Run ALL functional tests
```

**Docker Commands (3-Node Cluster):**
```bash
make cluster-start        # Start 3-node cluster with persistent volumes
make cluster-stop         # Stop 3-node cluster
make cluster-test         # Test cluster connectivity and tables
make cluster-logs         # Show logs from all 3 nodes
```

**Schema Management:**
```bash
make schema-update        # Update hard links from api schema files
make schema-verify        # Verify hard links are correct
```

**Development:**
```bash
make info                 # Show configuration information
make status               # Check build and container status
```

#### Example Workflow

```bash
# 1. Build Docker image
make docker-build

# 2. Start container (creates /tmp/clickhouse-test with persistent data)
make docker-run

# 3. Wait 60 seconds for full initialization, then verify
make docker-verify-tables
# Should show: 236 tables

# 4. Test events table
make docker-test-events
# Sends 5 test events, waits for batch flush, queries results

# 5. Test messaging tables (mthreads, mstore, mtriage)
make docker-test-messaging
# Sends conversion event, verifies all 3 messaging tables

# 6. View logs
make docker-logs

# 7. Open ClickHouse client for manual queries
make docker-clickhouse-shell
# Then run: SELECT count() FROM sfpla.events FINAL;

# 8. Clean up
make docker-stop
```

#### Testing Messaging Tables

The fixed messaging tables (`mthreads`, `mstore`, `mtriage`) now properly map to the actual ClickHouse schema:

**mthreads** (Thread metadata - 140+ columns):
- Core fields: `tid`, `alias`, `xstatus`, `name`, `provider`, `medium`
- Campaign tracking: `campaign_id`, `campaign_status`, `campaign_priority`
- A/B testing: 20+ `abz_*` fields
- Attribution: `attribution_model`, `attribution_weight`

**mstore** (Permanent message archive - 47 columns):
- Message content: `mid`, `subject`, `msg`, `data`
- Delivery: `urgency`, `sys`, `broadcast`, `svc`
- Timing: `planned`, `scheduled`, `started`, `completed`
- Performance: `interest` (JSON), `perf` (JSON)

**mtriage** (Messages in triage - 43 columns, identical to mstore except no `planned` field):
- Same structure as mstore but for messages being processed
- Default `urgency=8` for high-priority triage

Test with:
```bash
make docker-test-messaging
```

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

#### Mac
```sh
brew install go
go build
./tracker config.json
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
curl -w "\n" -k -H 'Content-Type: application/json'  -XPOST  "https://localhost:8443/tr/v1/tr/" -d '{"hideFreePlan":"false","name":"Bewusstsein in Aufruhr","newsletter":"bewusstsein-in-aufruhr","static":"%2Fkurs%2Fbewusstsein-in-aufruhr","umleitung":"%2Fkurs%2Fbewusstsein-in-aufruhr","ename":"visited_site","etyp":"session","last":"/einloggen","url":"/registrieren","ptyp":"logged_out_ancillary","sid":"627f7c80-0d7c-11eb-9767-93f1d9c02a9c","first":"true","tz":"America/Los_Angeles","device":"Mac","os":"macOS","w":1331,"h":459,"vid":"627f7c80-0d7c-11eb-9767-93f1d9c02a9c","rel":"1.0.179","app":"hd","params":{"hideFreePlan":"false","name":"Bewusstsein in Aufruhr","newsletter":"bewusstsein-in-aufruhr","static":"%2Fkurs%2Fbewusstsein-in-aufruhr","umleitung":"%2Fkurs%2Fbewusstsein-in-aufruhr","ename":"viewed_page","etyp":"view","last":"/einloggen","url":"/registrieren","ptyp":"logged_out_ancillary","sid":"627f7c80-0d7c-11eb-9767-93f1d9c02a9c","first":"true","tz":"America/Los_Angeles","device":"Mac","os":"macOS","w":1331,"h":459,"vid":"627f7c80-0d7c-11eb-9767-93f1d9c02a9c","rel":"1.0.179","app":"hd","homepageSlogan":"B","homepagePricePlans":"A"}}'

#or check ltv
curl -w "\n" -k -H 'Content-Type: application/json'  -XPOST  "https://localhost:8443/tr/v1/ltv/" -d '{"vid":"627f7c80-0d7c-11eb-9767-93f1d9c02a9c","uid":"627f7c80-0d7c-11eb-9767-93f1d9c02a9c","sid":"627f7c80-0d7c-11eb-9767-93f1d9c02a9c", "orid":"627f7c80-0d7c-11eb-9767-93f1d9c02a9c", "amt" : 35}'

#or privacy
curl -w "\n" -k -H 'Content-Type: application/json' -XPOST  "https://localhost:8443/tr/v1/ppi/agree" -d '{"vid": "5ae3c890-5e55-11ea-9283-4fa18a847130", "cflags": 1024}'
```


### Testing ClickHouse

#### Unit Tests

```bash
go test -v -run TestBatchWrite tests/batch_write_test.go
```

Note: You may need to flush the insert queue and wait for 2 seconds before querying the table.

```bash
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" && sleep 2 && clickhouse client --query "SELECT COUNT(*) FROM sfpla.events"
```

#### Functional Tests - All Endpoints

##### 1. Track Event (Client-side)
```bash
# REST/URL format
curl -k "https://localhost:8443/tr/v1/tr/vid/14fb0860-b4bf-11e9-8971-7b80435315ac/ename/page_view/etyp/view/first/true"

# JSON format
curl -k -H 'Content-Type: application/json' -X POST \
  "https://localhost:8443/tr/v1/tr/" \
  -d '{
    "vid": "14fb0860-b4bf-11e9-8971-7b80435315ac",
    "sid": "14fb0860-b4bf-11e9-8971-7b80435315ac",
    "ename": "page_view",
    "etyp": "view",
    "url": "https://example.com/page",
    "first": "false",
    "tz": "America/Los_Angeles",
    "device": "Desktop",
    "os": "macOS"
  }'

# Verify
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" && sleep 2
clickhouse client --query "SELECT ename, etyp, vid FROM sfpla.events ORDER BY created_at DESC LIMIT 5"
```

##### 2. Track Event (Server-side)
```bash
# Server-side tracking (returns event ID)
curl -k -H 'Content-Type: application/json' -X POST \
  "https://localhost:8443/tr/v1/str/" \
  -d '{
    "vid": "14fb0860-b4bf-11e9-8971-7b80435315ac",
    "oid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "ename": "server_event",
    "etyp": "conversion",
    "revenue": "99.99"
  }'

# Verify
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" && sleep 2
clickhouse client --query "SELECT ename, etyp, vid, oid FROM sfpla.events WHERE etyp='conversion' ORDER BY created_at DESC LIMIT 5"
```

##### 3. Track Lifetime Value (LTV)
```bash
# Single payment
curl -k -H 'Content-Type: application/json' -X POST \
  "https://localhost:8443/tr/v1/ltv/" \
  -d '{
    "vid": "14fb0860-b4bf-11e9-8971-7b80435315ac",
    "uid": "user-123",
    "oid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "amt": 99.99,
    "currency": "USD",
    "orid": "order-123"
  }'

# Batch payments
curl -k -H 'Content-Type: application/json' -X POST \
  "https://localhost:8443/tr/v1/ltv/" \
  -d '{
    "vid": "14fb0860-b4bf-11e9-8971-7b80435315ac",
    "uid": "user-123",
    "oid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "payments": [
      {"amt": 50.00, "currency": "USD", "orid": "order-124"},
      {"amt": 25.00, "currency": "USD", "orid": "order-125"}
    ]
  }'

# Verify LTV tables
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" && sleep 2
clickhouse client --query "SELECT vid, uid, revenue FROM sfpla.ltv ORDER BY updated_at DESC LIMIT 5"
clickhouse client --query "SELECT uid, revenue FROM sfpla.ltvu ORDER BY updated_at DESC LIMIT 5"
clickhouse client --query "SELECT vid, revenue FROM sfpla.ltvv ORDER BY updated_at DESC LIMIT 5"
```

##### 4. Redirect/Short URL API

```bash
# Create a shortened URL
curl -k -H 'Content-Type: application/json' -X POST \
  "https://localhost:8443/tr/v1/rpi/redirect/14fb0860-b4bf-11e9-8971-7b80435315ac/password" \
  -d '{
    "urlfrom": "https://yourdomain.com/short",
    "hostfrom": "yourdomain.com",
    "slugfrom": "/short",
    "urlto": "https://example.com/long/path?utm_source=test",
    "hostto": "example.com",
    "pathto": "/long/path",
    "searchto": "?utm_source=test",
    "oid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
  }'

# Get all redirects for a host
curl -k -X GET \
  "https://localhost:8443/tr/v1/rpi/redirects/14fb0860-b4bf-11e9-8971-7b80435315ac/password/yourdomain.com"

# Test the redirect (visit in browser or curl)
curl -k -L "https://localhost:8443/short"

# Verify in database
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" && sleep 2
clickhouse client --query "SELECT urlfrom, urlto FROM sfpla.redirects LIMIT 10"
clickhouse client --query "SELECT urlfrom, urlto, updater FROM sfpla.redirect_history ORDER BY updated_at DESC LIMIT 10"
```

##### 5. Privacy/Agreement API

```bash
# Post user agreement (GDPR consent)
curl -k -H 'Content-Type: application/json' -X POST \
  "https://localhost:8443/tr/v1/ppi/agree" \
  -d '{
    "vid": "14fb0860-b4bf-11e9-8971-7b80435315ac",
    "cflags": 1024,
    "tz": "America/Los_Angeles",
    "lat": 37.7749,
    "lon": -122.4194,
    "oid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
  }'

# Get agreements for a visitor
curl -k -X GET \
  "https://localhost:8443/tr/v1/ppi/agree?vid=14fb0860-b4bf-11e9-8971-7b80435315ac"

# Verify
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" && sleep 2
clickhouse client --query "SELECT vid, cflags, country FROM sfpla.agreements ORDER BY created_at DESC LIMIT 5"
clickhouse client --query "SELECT vid, cflags, country FROM sfpla.agreed ORDER BY created_at DESC LIMIT 10"
```

##### 6. Get Jurisdictions

```bash
# Get all jurisdictions (privacy regions)
curl -k -X GET "https://localhost:8443/tr/v1/ppi/jds"

# Verify
clickhouse client --query "SELECT * FROM sfpla.jurisdictions LIMIT 10"
```

##### 7. GeoIP Lookup

```bash
# Get GeoIP for current IP
curl -k -X GET "https://localhost:8443/tr/v1/ppi/geoip"

# Get GeoIP for specific IP
curl -k -X GET "https://localhost:8443/tr/v1/ppi/geoip?ip=8.8.8.8"
```

##### 8. WebSocket Streaming (LZ4 Compressed)

```javascript
// JavaScript example (run in browser console)
const ws = new WebSocket('wss://localhost:8443/tr/v1/ws');

ws.onopen = () => {
  // Send uncompressed JSON
  ws.send(JSON.stringify({
    vid: '14fb0860-b4bf-11e9-8971-7b80435315ac',
    ename: 'websocket_event',
    etyp: 'test'
  }));

  // Send LZ4 compressed binary (if you have lz4 library)
  // const compressed = lz4.compress(JSON.stringify(data));
  // ws.send(compressed);
};

ws.onmessage = (event) => {
  console.log('Received:', event.data);
};
```

##### 9. Campaign/Message Thread Updates

```bash
# Track campaign event (goes to mthreads, mstore, mtriage)
curl -k -H 'Content-Type: application/json' -X POST \
  "https://localhost:8443/tr/v1/str/" \
  -d '{
    "vid": "14fb0860-b4bf-11e9-8971-7b80435315ac",
    "oid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "tid": "thread-123",
    "campaign_id": "campaign-456",
    "experiment_id": "exp-789",
    "variant_id": "var-abc",
    "ename": "email_sent",
    "etyp": "message",
    "subject": "Test Email",
    "content": "Email body content",
    "status": "sent",
    "channel": "email"
  }'

# Verify message thread tables
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" && sleep 2
clickhouse client --query "SELECT tid, campaign_id, status, channel FROM sfpla.mthreads ORDER BY updated_at DESC LIMIT 5"
clickhouse client --query "SELECT tid, subject, content FROM sfpla.mstore ORDER BY updated_at DESC LIMIT 5"
clickhouse client --query "SELECT tid, status FROM sfpla.mtriage ORDER BY updated_at DESC LIMIT 5"
```

##### 10. Health & Metrics Endpoints

```bash
# Health check
curl -k "https://localhost:8443/health"

# Ping endpoint
curl -k "https://localhost:8443/ping"

# Metrics endpoint (Prometheus format)
curl -k "https://localhost:8443/metrics"

# Status endpoint
curl -k "https://localhost:8443/status"
```

##### 11. Batch Testing - High Volume

```bash
# Send 100 events rapidly to test batching
for i in {1..100}; do
  curl -k -H 'Content-Type: application/json' -X POST \
    "https://localhost:8443/tr/v1/tr/" \
    -d "{
      \"vid\": \"batch-test-$i\",
      \"ename\": \"batch_event_$i\",
      \"etyp\": \"test\",
      \"batch_num\": \"$i\"
    }" &
done
wait

# Wait for batches to flush
sleep 5
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" && sleep 2

# Verify batch inserts
clickhouse client --query "SELECT COUNT(*) as total, etyp FROM sfpla.events WHERE etyp='test' GROUP BY etyp"
```

##### 12. Complete End-to-End Test

```bash
#!/bin/bash
# Complete workflow test

VID="e2e-$(uuidgen)"
UID="user-$(uuidgen)"
OID="org-$(uuidgen)"

echo "=== Testing with VID: $VID ==="

# 1. First visit (page view)
echo "1. Page view..."
curl -sk -H 'Content-Type: application/json' -X POST \
  "https://localhost:8443/tr/v1/tr/" \
  -d "{\"vid\":\"$VID\",\"ename\":\"page_view\",\"etyp\":\"view\",\"first\":\"true\"}"

# 2. User signs up
echo "2. Signup..."
curl -sk -H 'Content-Type: application/json' -X POST \
  "https://localhost:8443/tr/v1/str/" \
  -d "{\"vid\":\"$VID\",\"uid\":\"$UID\",\"oid\":\"$OID\",\"ename\":\"signup\",\"etyp\":\"conversion\"}"

# 3. User makes purchase
echo "3. Purchase..."
curl -sk -H 'Content-Type: application/json' -X POST \
  "https://localhost:8443/tr/v1/ltv/" \
  -d "{\"vid\":\"$VID\",\"uid\":\"$UID\",\"oid\":\"$OID\",\"amt\":149.99}"

# 4. User agrees to terms
echo "4. Agreement..."
curl -sk -H 'Content-Type: application/json' -X POST \
  "https://localhost:8443/tr/v1/ppi/agree" \
  -d "{\"vid\":\"$VID\",\"cflags\":1024,\"oid\":\"$OID\"}"

# Wait for async inserts
sleep 3
clickhouse client --query "SYSTEM FLUSH ASYNC INSERT QUEUE" && sleep 2

# Verify all tables
echo ""
echo "=== Results ==="
echo "Events:"
clickhouse client --query "SELECT ename, etyp FROM sfpla.events WHERE vid='$VID' ORDER BY created_at"
echo ""
echo "LTV:"
clickhouse client --query "SELECT revenue FROM sfpla.ltv WHERE vid='$VID'"
echo ""
echo "Agreements:"
clickhouse client --query "SELECT cflags FROM sfpla.agreements WHERE vid='$VID'"
```

#### Cleaning up the database

```bash
clickhouse client --query "DROP DATABASE sfpla"
clickhouse keeper-client --host 0.0.0.0 --port 2181 --query "rmr '/clickhouse'"
clickhouse client < schema.1.ch.sql
```

#### Querying the database

```sql
SELECT
    dynamicType(params.go),
    params, params.go
FROM sfpla.events where params.go = 6;
```

From duckdb:

```sql
SELECT * FROM ch_scan("SELECT number * 2 FROM numbers(10)", "http://localhost:8123", user:='default', format:='parquet');
```
