#!/bin/bash

docker build --force-rm=true --no-cache=true --build-arg DB_EDITION="xe" -t "oracle/database:18.4.0-xe" -f Dockerfile.xe .
