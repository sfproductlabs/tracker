# DragonGate
A pure Go lang replacement for NGINX in one page of code with proxying, tracking urls, file server, benchmarks, templates and automatic TLS support using LetsEncrypt (or normal certificates or using AWS ECS secure parameters). A docker deployment option is also mentioned below.

## Instructions

### Setup
* Install go and dependencies, requires Go +1.8 (on debian buster):
```
apt install golang-1.8-go git libssl-dev
```
* Add the following to your ~/.bashrc:
```
export GOROOT=/usr/lib/go-1.8
export GOPATH=$HOME/go
export PATH=$PATH:$GOROOT/bin:$GOPATH/bin
```
* Get, build and run the sources (change the config.json to your liking):
```
go get github.com/dioptre/tracker
cd ~/go/src/github.com/dioptre/tracker
go install
./debug.sh
```
* You may want to change the config.json
```javascript
{
    "Domains": ["dev.sfproductlabs.com"],
    "ProxyUrl": "http://localhost:8000",
    "StaticDirectory" : "public",
    "UsePingPong" : true,
    "Tracker" : "/tracker",
    "UseLocalTLS" : false
}
```
   * Domains **is required** and is what domains are allowed, and connect automatically to LetsEncrypt.
   * ProxyUrl (optional) if included proxies connections to a Node.js or other local web service.
   * StaticDirectory (optional) if included serves static files from this subdirectory
   * UsePingPong (optional) if included responds "pong" to https://yourdomain.com/ping
   * Tracker (optional) if included responds a transparent image (wip).
   * UseLocalTLS (optional) if included and **true** elects to use server.crt, and server.key instead of LetsEncrypt.

### Troubleshooting
I had to disable ipv6 (error during LetsEncrypt init phase) also, this has been done in the docker image also.

* Edit /etc/sysctl.conf:
```
net.ipv6.conf.enp6s0.disable_ipv6 = 1
```

### Tests
* Tested on ARM and x64.

* Benchmarking run:
```
./benchmark.sh https://yourservername.com/ping
```
My results are around 1,000 requests per second per $ per server per month ([@dioptre](https://github.com/dioptre/)).
* Run a test server to proxy to:
```
./proxy_server.sh
```
* Test key (outside of LetsEncrypt)
```
# You can also use the helper genkey.sh
openssl ecparam -genkey -name secp384r1 -out server.key
openssl req -new -x509 -sha256 -key server.key -out server.crt -days 3650
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
* [@psytron](https://github.com/psytron)
* [@dioptre](https://github.com/dioptre)
* [SF Product Labs](https://sfproductlabs.com)

