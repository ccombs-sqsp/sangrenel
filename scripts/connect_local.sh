#!/bin/bash

go build -o bin
./bin/sangrenel -api-version 2.7.0 -brokers localhost:9092 -topic golang
