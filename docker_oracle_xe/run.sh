#!/bin/bash

docker run --name oracledb -p 1521:1521 -p 5500:5500 -e ORACLE_PWD=root "oracle/database:18.4.0-xe"
