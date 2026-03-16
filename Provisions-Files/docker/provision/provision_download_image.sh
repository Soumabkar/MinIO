#!/bin/bash
images=(
  "minio/minio:latest"
  "minio/mc:latest"
  "postgres:18.0-alpine"
  "bitsondatadev/hive-metastore:latest"
  "trinodb/trino:477"
)

# image : jupyter "jupyter/pyspark-notebook:latest"
# image : hive "apache/hive:4.0.0"
# image "trinodb/trino:406"
# image : "postgres:18.0-alpine"

# Pull des images Docker avec retry en cas d'échec de téléchargement (utile pour les connexions instables)  
# Chaque image sera tentée jusqu'à 5 fois avec une pause de 10 secondes entre les tentatives
# Note : Assurez-vous que Docker est installé et que le service Docker est en cours d'exécution avant d'exécuter ce script
# bitsondatadev/hive-metastore si "apache/hive:4.0.0" pose problème (problème de téléchargement ou de compatibilité avec la version de Hive utilisée)

for image in "${images[@]}"; do
  echo "Pulling $image..."
  for i in 1 2 3 4 5; do
    docker pull "$image" && break
    echo "Retry $i pour $image..."
    sleep 10
  done
done