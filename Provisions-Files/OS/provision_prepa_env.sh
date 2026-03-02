#!/bin/bash

sudo apt-get update -y
sudo apt-get install -y software-properties-common apt-transport-https ca-certificates curl  gnupg-agent lsb-release

sudo mkdir -p /home/vagrant/project/python/MinIO/data/
sudo chown -R vagrant:vagrant /home/vagrant/project/python/MinIO/
sudo chmod -R 755 /home/vagrant/project/python/MinIO/
sudo chown -R vagrant:vagrant /home/vagrant/project/python/MinIO/data/
sudo chmod -R 755 /home/vagrant/project/python/MinIO/data/

sudo mkdir -p /home/vagrant/project/scala/MinIO
sudo chown -R vagrant:vagrant /home/vagrant/project/scala/MinIO
sudo chmod -R 755 /home/vagrant/project/scala/MinIO

sudo mkdir -p /home/vagrant/docker/dockerfile
sudo chown -R vagrant:vagrant /home/vagrant/docker/dockerfile
sudo chmod -R 755 /home/vagrant/docker/dockerfile

sudo mkdir -p /home/vagrant/docker/dockerfile/trino-config
sudo chown -R vagrant:vagrant /home/vagrant/docker/dockerfile/trino-config
sudo chmod -R 755 /home/vagrant/docker/dockerfile/trino-config

sudo mkdir -p /home/vagrant/docker/dockerfile/trino-config/catalog
sudo chown -R vagrant:vagrant /home/vagrant/docker/dockerfile/trino-config/catalog
sudo chmod -R 755 /home/vagrant/docker/dockerfile/trino-config/catalog


