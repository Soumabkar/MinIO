#!/bin/bash

# Vérifier si minikube est installé
echo "=== Vérification de Minikube ==="

if command -v minikube &> /dev/null; then
    echo "Minikube est installé : $(minikube version)"
    # Vérifier l'état du cluster
    echo -e "\n=== État du cluster Minikube ==="
    minikube status || echo "Aucun cluster Minikube en cours d'exécution."
else
    echo "Minikube n'est PAS installé."
    echo "Installation de Minikube via arkade..."
    # Installation de minikube via arkade [si arkade est installé au préalable (voir provision_instal_arkade.sh)]
    arkade get minikube # Installation de minikube via arkade
    sudo mv /$HOME/.arkade/bin/minikube /usr/local/bin/ #sudo mv /$HOME/.arkade/bin/minikube /usr/local/bin/
    sudo chmod +x /usr/local/bin/minikube
    echo "Minikube a été installé : $(sudo minikube version)"
 
fi

