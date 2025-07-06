#!/bin/bash

cd service
cd grafana
cp -f ../common/config.env ./
make all
rm config.env
cd ..
cd prometheus
cp -f ../common/config.env ./
make all
rm config.env
cd ..
cd raydp_base
cp -f ../common/config.env ./
make all
rm config.env
cd ..
cd raydp_head
cp -f ../common/config.env ./
make all
rm config.env
cd ..
cd raydp_worker
cp -f ../common/config.env ./
make all
rm config.env
cd ..
cd raydp_custom_worker
cp -f ../common/config.env ./
make all
rm config.env
cd ..