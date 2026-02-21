#!/bin/bash
# Vérifier si Docker est installé
if command -v docker &> /dev/null; then
    echo "Docker est installé."
    echo "Version : $(docker --version)"
    echo "Docker service status : $(systemctl is-active docker)"
else
    echo "Docker n'est pas installé."
    echo "Installation de Docker..."
    # Mettre à jour les paquets existants
    sudo apt-get update -y
    sudo apt-get install -y apt-transport-https ca-certificates curl software-properties-common gnupg-agent
    # Récupération de la clé du dépôt de paquets Docker
    sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
    # Ajout du dépôt Docker aux sources APT
    sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
    # Installation de Docker
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io

    sudo getent group docker >/dev/null || sudo groupadd docker #sudo groupadd docker
    sudo usermod -aG docker vagrant  # id vagrant, sudo usermod -aG docker $USER
    newgrp docker  # Recharge les groupes pour la session actuelle
    sudo systemctl restart docker
    
fi
      
