#!/bin/bash
go get github.com/dioptre/tracker
go install github.com/dioptre/tracker
go build
sudo docker build -t tracker .
