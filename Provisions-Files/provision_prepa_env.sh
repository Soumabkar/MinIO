#!/bin/bash

sudo apt-get update -y
sudo apt-get install -y apt-transport-https ca-certificates curl software-properties-common gnupg-agent lsb-release python3-pip

pip3 install minio