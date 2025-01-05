#!/bin/bash

# Env variables
export PROJECT_ID=phading-dev
export INSTANCE_ID=test-instance
export TABLE_ID=SINGLE

# GCP auth
gcloud auth application-default login

# BigTable
cbt -project phading-dev createinstance test-instance "Test instance" test-instance-c1 us-central1-a 1 SSD
cbt -project phading-dev -instance test-instance createtable SINGLE
cbt -project phading-dev -instance test-instance createfamily SINGLE w:maxversions=1
cbt -project phading-dev -instance test-instance createfamily SINGLE a:maxversions=1
cbt -project phading-dev -instance test-instance createfamily SINGLE s:maxversions=1
cbt -project phading-dev -instance test-instance createfamily SINGLE u:maxversions=1
cbt -project phading-dev -instance test-instance createfamily SINGLE t:maxversions=1
cbt -project phading-dev -instance test-instance createfamily SINGLE c:maxversions=1
