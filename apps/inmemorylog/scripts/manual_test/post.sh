#!/usr/bin/env sh

curl -X POST localhost:8080 -d "{\"record\": {\"value\": \"${1}\"}}"