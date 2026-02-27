#!/bin/bash

sudo apt-get update -y
sudo apt-get install -y apt-transport-https ca-certificates curl software-properties-common gnupg-agent lsb-release

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

