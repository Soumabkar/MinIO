# Projet Datawaeahouse

## Mise en place de la plateform

Executez la commande `vagrant up` pour démarrer la VM et faire les installation automatiquement si c'est votre prémier fois de lancer. 
Les principales commandes de vagrant qu'on pourrait avoir besoins sont :
* `vagrant up` pour démarrer la vm
* `vagrant ssh` pour se connecter à la vm
* `vagrant provision` pour approvisionner la vm
* `vagrant halt` pour l'arrêter
* `vagrant destroy` pour supprimer la vm

###  Les services du datawarehouse

* MinIO(Storage compatible S3)
* Hive(Catalogue)
* Metastore(PostgreSQL)
* Trino(Moteur SQL)

Maintenant vous allez démarrer la vm `vagrant up` (exécuter où se trouve le fichier Vagrantfile) puis vous vous connectez vous à la vm `vagrant ssh`.
Une fois vous êtes connecté(e) à la vm, vous mettez les services en marche grâce à la commande suivante :

```bash
docker compose -f docker/dockerfile/docker-compose-datalake.yml up
```

### Trino

Une fois que les service sont en marche  vous pouvez avoir accès à Trino (en précisant le server, le catalogue et le schéma)

```bash
docker exec -it trino bash
trino --server localhost:8080 --catalog hive --schema ecommerce
show tables;
```

## Projet ecommerce en Python

### Mise en place de l'environnement virtuel

```bash
python3 -m venv venv
source venv/bin/activate
```

### Installation des dépendances

```bash
pip install -r requirements.txt
```

### Les variables d'environnement 

```
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=*********
MINIO_SECRET_KEY=*********
MINIO_BUCKET=warehouse
MINIO_SESSION_TOKEN=
MINIO_SECURE=False
MINIO_REGION=
TRINO_HOST=localhost
TRINO_PORT=8080
TRINO_USER=admin
TRINO_CATALOG=hive
TRINO_SCHEMA=ecommerce
SPARK_MASTER=local[*]
```

Exécutez la commande : `export $(grep -v '^#' conf/env | xargs)` pour que les variable d'environnement qui se trouvent le répertoire conf du projet soient sourcées.


