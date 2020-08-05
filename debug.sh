#!/bin/bash
#rm -rf pdb && rm -rf pdbwal && mkdir pdb && mkdir pdbwal && echo "*" >> pdb/.gitignore && echo "*" >> pdbwal/.gitignore && rm -rf tmp && mkdir tmp && echo "*" >> tmp/.gitignore 
go build && ./tracker
