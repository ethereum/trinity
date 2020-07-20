#!/usr/bin/env bash

sudo apt-get install influxdb;
sudo apt-get install influxdb-client;
(sudo influxd -config /etc/influxdb/influxdb.conf) &
# wait until influxdb service is actually available on port 8086
(while netstat -lnt | awk '$4 ~ /:8086$/ {exit 1}'; do sleep 5; done;
sudo influx --execute "CREATE DATABASE trinity";
sudo influx --execute "USE trinity";
sudo influx --execute "CREATE USER \"trinity\" WITH PASSWORD 'trinity' WITH ALL PRIVILEGES") &
(sleep infinity;)
