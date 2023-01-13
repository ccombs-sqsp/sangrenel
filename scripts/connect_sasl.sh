#!/bin/bash

./bin/sangrenel -api-version 3.1.0 -brokers bootstrap.dev-kafka.eqx.dal.sandbox.squarespace.net:443 -topic dev1 -sasl -scram-algorithm SCRAM-SHA-512 -username "$WARBOY_USERNAME" -password "$WARBOY_PASSWORD"