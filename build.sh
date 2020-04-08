#!/bin/bash
go get github.com/sfproductlabs/tracker
go install github.com/sfproductlabs/tracker
go build
sudo docker build -t tracker .
