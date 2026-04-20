#!/bin/bash

sudo apt-get update -y
sudo apt-get install -y software-properties-common apt-transport-https ca-certificates curl  gnupg-agent lsb-release

sudo mkdir -p /home/vagrant/project/scala/minio/
sudo chown -R vagrant:vagrant /home/vagrant/project/scala/minio/
sudo chmod -R 755 /home/vagrant/project/scala/minio/

sudo mkdir -p /home/vagrant/project/scala/minio/src/main/scala/minio/sqlengine/
sudo chown -R vagrant:vagrant /home/vagrant/project/scala/minio/src/main/scala/minio/sqlengine/
sudo chmod -R 755 /home/vagrant/project/scala/minio/src/main/scala/minio/sqlengine/

sudo mkdir -p /home/vagrant/project/scala/minio/src/main/scala/minio/
sudo chown -R vagrant:vagrant /home/vagrant/project/scala/minio/src/main/scala/minio/
sudo chmod -R 755 /home/vagrant/project/scala/minio/src/main/scala/minio/

sudo mkdir -p /home/vagrant/project/scala/minio/src/main/scala/minio/config/
sudo chown -R vagrant:vagrant /home/vagrant/project/scala/minio/src/main/scala/minio/config/
sudo chmod -R 755 /home/vagrant/project/scala/minio/src/main/scala/minio/config/

sudo mkdir -p /home/vagrant/project/scala/minio/src/main/scala/minio/datawarehouse/
sudo chown -R vagrant:vagrant /home/vagrant/project/scala/minio/src/main/scala/minio/datawarehouse/
sudo chmod -R 755 /home/vagrant/project/scala/minio/src/main/scala/minio/datawarehouse/

sudo mkdir -p /home/vagrant/project/scala/minio/src/main/scala/minio/entity/
sudo chown -R vagrant:vagrant /home/vagrant/project/scala/minio/src/main/scala/minio/entity/
sudo chmod -R 755 /home/vagrant/project/scala/minio/src/main/scala/minio/entity/

sudo mkdir -p /home/vagrant/project/scala/minio/src/main/scala/minio/project/
sudo chown -R vagrant:vagrant /home/vagrant/project/scala/minio/src/main/scala/minio/project/
sudo chmod -R 755 /home/vagrant/project/scala/minio/src/main/scala/minio/project/

sudo mkdir -p /home/vagrant/project/scala/minio/src/main/scala/minio/spark/
sudo chown -R vagrant:vagrant /home/vagrant/project/scala/minio/src/main/scala/minio/spark/
sudo chmod -R 755 /home/vagrant/project/scala/minio/src/main/scala/minio/spark/

sudo mkdir -p /home/vagrant/project/scala/minio/src/main/scala/minio/sqlengine/
sudo chown -R vagrant:vagrant /home/vagrant/project/scala/minio/src/main/scala/minio/sqlengine/
sudo chmod -R 755 /home/vagrant/project/scala/minio/src/main/scala/minio/sqlengine/

# sudo mkdir -p /home/vagrant/docker/dockerfile
# sudo chown -R vagrant:vagrant /home/vagrant/docker/dockerfile
# sudo chmod -R 755 /home/vagrant/docker/dockerfile

# sudo mkdir -p /home/vagrant/docker/dockerfile/trino-config
# sudo chown -R vagrant:vagrant /home/vagrant/docker/dockerfile/trino-config
# sudo chmod -R 755 /home/vagrant/docker/dockerfile/trino-config

# sudo mkdir -p /home/vagrant/docker/dockerfile/trino-config/catalog
# sudo chown -R vagrant:vagrant /home/vagrant/docker/dockerfile/trino-config/catalog
# sudo chmod -R 755 /home/vagrant/docker/dockerfile/trino-config/catalog





