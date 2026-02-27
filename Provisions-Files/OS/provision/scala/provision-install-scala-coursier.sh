#!/bin/bash

# Mettre à jour les paquets et installer les dépendances nécessaires
sudo apt-get update
sudo apt-get install -y curl unzip

# Télécharger et installer Coursier
curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs
chmod +x cs
./cs setup