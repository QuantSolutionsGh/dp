#!/bin/bash

rm -f upsert-event-aggregations.zip
pip3 install -r requirements.txt --target package/ --system
cd package
zip -r9 ../upsert-event-aggregations.zip .
cd ..
zip -g upsert-event-aggregations.zip upsert-event-aggregations.py
