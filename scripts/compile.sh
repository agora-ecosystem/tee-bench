#!/bin/bash

cd ..
make clean
make sgx
./app
cd scripts