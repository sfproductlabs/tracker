#!/bin/bash
go get github.com/sfproductlabs/tracker
go install github.com/sfproductlabs/tracker
rm -rf pdb && rm -rf pdbwal && mkdir pdb && mkdir pdbwal && echo "*" >> pdb/.gitignore && echo "*" >> pdbwal/.gitignore && rm -rf tmp && mkdir tmp && echo "*" >> tmp/.gitignore
go build
sudo docker build -t tracker .
