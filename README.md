# DragonGate
Pure Go lang implementation of NGINX functionality with SSL management, static file serving, and HTTP reverse proxy to Python or other process

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
go get github.com/dioptre/dragongate
cd ~/go/src/github.com/dioptre/dragongate
go install
./build.sh
```

### Misc.
* Update your config parameters in conjunction with KMS on ECS on Amazon AWS https://hackernoon.com/you-should-use-ssm-parameter-store-over-lambda-env-variables-5197fc6ea45b


### Troubleshooting
I had to disable ipv6 (error during LetsEncrypt init phase) also.

* Edit /etc/sysctl.conf:
```
net.ipv6.conf.enp6s0.disable_ipv6 = 1
```

### Tests

* Benchmarking run:
```
./benchmark.sh
```


