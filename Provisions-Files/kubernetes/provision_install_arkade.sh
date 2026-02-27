#!/bin/bash

if command -v arkade &> /dev/null; then
    echo "arkade est déjà installé : $(arkade version)"
else
    echo "arkade n'est pas installé."
    echo "Installation de arkade..."
    curl -sLS https://get.arkade.dev | sudo sh
    echo "arkade a été installé : $(arkade version)"

    # Ajouter Arkade au PATH si nécessaire
    if [ -d "$HOME/.arkade/bin" ]; then
        echo 'export PATH=$PATH:~/.arkade/bin/' >> ~/.bashrc
        source ~/.bashrc
    fi

    # Vérifier l'installation
    if command -v arkade &> /dev/null; then
        echo "Arkade a été installé avec succès :"
        arkade version
    else
        echo "Échec de l'installation d'Arkade. Vérifie les logs."
        exit 1
    fi

    #export PATH=$PATH:$HOME/.arkade/bin/ # Ajout de arkade au PATH
fi