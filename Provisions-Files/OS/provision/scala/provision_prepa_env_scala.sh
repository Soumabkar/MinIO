#!/bin/bash

sudo apt-get update -y
sudo apt-get install -y software-properties-common apt-transport-https ca-certificates curl  gnupg-agent lsb-release

sudo mkdir -p /home/vagrant/project/scala/MinIO/
sudo chown -R vagrant:vagrant /home/vagrant/project/scala/MinIO/
sudo chmod -R 755 /home/vagrant/project/scala/MinIO/

sudo mkdir -p /home/vagrant/project/scala/MinIO/config/
sudo chown -R vagrant:vagrant /home/vagrant/project/scala/MinIO/config/
sudo chmod -R 755 /home/vagrant/project/scala/MinIO/config/

sudo mkdir -p /home/vagrant/project/scala/MinIO/datawarehouse/
sudo chown -R vagrant:vagrant /home/vagrant/project/scala/MinIO/datawarehouse/
sudo chmod -R 755 /home/vagrant/project/scala/MinIO/datawarehouse/

sudo mkdir -p /home/vagrant/project/scala/MinIO/entity/
sudo chown -R vagrant:vagrant /home/vagrant/project/scala/MinIO/entity/
sudo chmod -R 755 /home/vagrant/project/scala/MinIO/entity/

sudo mkdir -p /home/vagrant/project/scala/MinIO/project/
sudo chown -R vagrant:vagrant /home/vagrant/project/scala/MinIO/project/
sudo chmod -R 755 /home/vagrant/project/scala/MinIO/project/

sudo mkdir -p /home/vagrant/project/scala/MinIO/spark/
sudo chown -R vagrant:vagrant /home/vagrant/project/scala/MinIO/spark/
sudo chmod -R 755 /home/vagrant/project/scala/MinIO/spark/

sudo mkdir -p /home/vagrant/project/scala/MinIO/sqlengine/
sudo chown -R vagrant:vagrant /home/vagrant/project/scala/MinIO/sqlengine/
sudo chmod -R 755 /home/vagrant/project/scala/MinIO/sqlengine/

sudo mkdir -p /home/vagrant/docker/dockerfile
sudo chown -R vagrant:vagrant /home/vagrant/docker/dockerfile
sudo chmod -R 755 /home/vagrant/docker/dockerfile

sudo mkdir -p /home/vagrant/docker/dockerfile/trino-config
sudo chown -R vagrant:vagrant /home/vagrant/docker/dockerfile/trino-config
sudo chmod -R 755 /home/vagrant/docker/dockerfile/trino-config

sudo mkdir -p /home/vagrant/docker/dockerfile/trino-config/catalog
sudo chown -R vagrant:vagrant /home/vagrant/docker/dockerfile/trino-config/catalog
sudo chmod -R 755 /home/vagrant/docker/dockerfile/trino-config/catalog





