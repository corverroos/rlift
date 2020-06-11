#!/usr/bin/env bash

docker run -d --name=liftbridge-main -p 4222:4222 -p 9292:9292 -p 8222:8222 -p 6222:6222 liftbridge/standalone-dev
