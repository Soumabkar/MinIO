#!/bin/bash

# Vérification de l'installation de Helm
if command -v helm &> /dev/null; then
    echo "Helm est déjà installé :"
    helm version
else
    #install_helm
    arkade get helm # Installation de helm via arkade
    export PATH=$PATH:$HOME/.arkade/bin/
    sudo mv /$HOME/.arkade/bin/helm /usr/local/bin/ # sudo mv /home/vagrant/.arkade/bin/helm /usr/local/bin/ Déplacement de helm vers /usr/local/bin pour un accès global
    helm version
fi
