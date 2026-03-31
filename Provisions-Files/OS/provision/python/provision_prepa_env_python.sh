#!/bin/bash

sudo apt-get update -y
sudo apt-get install -y software-properties-common apt-transport-https ca-certificates curl  gnupg-agent lsb-release

sudo mkdir -p /home/vagrant/project/python/MinIO/
sudo chown -R vagrant:vagrant /home/vagrant/project/python/MinIO/
sudo chmod -R 755 /home/vagrant/project/python/MinIO/

sudo mkdir -p /home/vagrant/project/python/MinIO/data/
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

sudo mkdir -p /home/vagrant/project/python/MinIO/conf/
sudo chown -R vagrant:vagrant /home/vagrant/project/python/MinIO/conf/
sudo chmod -R 755 /home/vagrant/project/python/MinIO/conf/

sudo mkdir -p /home/vagrant/project/python/MinIO/Datawarehouse/
sudo chown -R vagrant:vagrant /home/vagrant/project/python/MinIO/Datawarehouse/
sudo chmod -R 755 /home/vagrant/project/python/MinIO/Datawarehouse/

sudo mkdir -p /home/vagrant/project/python/MinIO/Entity/
sudo chown -R vagrant:vagrant /home/vagrant/project/python/MinIO/Entity/
sudo chmod -R 755 /home/vagrant/project/python/MinIO/Entity/

sudo mkdir -p /home/vagrant/project/python/MinIO/Files/Input/
sudo chown -R vagrant:vagrant /home/vagrant/project/python/MinIO/Files/Input/
sudo chmod -R 755 /home/vagrant/project/python/MinIO/Files/Input/

sudo mkdir -p /home/vagrant/project/python/MinIO/Files/Output/
sudo chown -R vagrant:vagrant /home/vagrant/project/python/MinIO/Files/Output/
sudo chmod -R 755 /home/vagrant/project/python/MinIO/Files/Output/

sudo mkdir -p /home/vagrant/project/python/MinIO/main/
sudo chown -R vagrant:vagrant /home/vagrant/project/python/MinIO/main/
sudo chmod -R 755 /home/vagrant/project/python/MinIO/main/

sudo mkdir -p /home/vagrant/project/python/MinIO/Spark/
sudo chown -R vagrant:vagrant /home/vagrant/project/python/MinIO/Spark/
sudo chmod -R 755 /home/vagrant/project/python/MinIO/Spark/

sudo mkdir -p /home/vagrant/project/python/MinIO/SqlEngine/
sudo chown -R vagrant:vagrant /home/vagrant/project/python/MinIO/SqlEngine/
sudo chmod -R 755 /home/vagrant/project/python/MinIO/SqlEngine/

sudo mkdir -p /home/vagrant/project/python/MinIO/utils/
sudo chown -R vagrant:vagrant /home/vagrant/project/python/MinIO/utils/
sudo chmod -R 755 /home/vagrant/project/python/MinIO/utils/

sudo mkdir -p /home/vagrant/go/pkg/mod
sudo chown -R vagrant:vagrant /home/vagrant/go/pkg/mod
sudo chmod -R 755 /home/vagrant/go/pkg/mod




