# Projet Datawaeahouse

## Mise en place de la plateform

Executez la commande `vagrant up` pour démarrer la VM et faire les installation automatiquement si c'est votre prémier fois de lancer. 
Les principales commandes de vagrant qu'on pourrait avoir besoins sont :
* `vagrant up` pour démarrer la vm
* `vagrant ssh` pour se connecter à la vm
* `vagrant provision` pour approvisionner la vm
* `vagrant halt` pour l'arrêter
* `vagrant destroy` pour supprimer la vm
* `vagrant ssh -- -L xxxx:localhost:xxxx` pour faire correspondre un port ouvert sur la vm à la machine hote

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

#### Soucis PID résiduel dans son répertoire de données sur Trino

Nous pouvons avoir l'erreur r ERROR: already running as 13 signifie que Trino trouve un fichier PID résiduel dans son répertoire de données, qui lui fait croire qu'une instance tourne déjà.
C'est causé par le volume persistant trino-data:/data/trino qui conserve un fichier var/run/launcher.pid entre les redémarrages.

##### Solution 1 — Supprimer le volume (la plus simple)

* Arrêter les conteneurs : 
```bash
docker compose -f docker/dockerfile/docker-compose-datalake.yml down
```


* Supprimer le volume trino-data : 
docker volume rm dockerfile_trino-data

* Relancer : 
```bash
docker compose -f docker/dockerfile/docker-compose-datalake.yml up
```



##### Solution 2 — Supprimer le fichier PID sans toucher aux données

```bash
docker compose -f docker/dockerfile/docker-compose-datalake.yml down
```


* Trouver le nom exact du volume : 
```bash
docker volume ls | grep trino
```


* Supprimer uniquement le fichier PID via un conteneur temporaire : 
```bash
docker run --rm -v dockerfile_trino-data:/data alpine  sh -c "rm -f /data/var/run/launcher.pid"

docker compose -f docker/dockerfile/docker-compose-datalake.yml up
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
SPARK_MASTER="local[*]"
```

Exécutez la commande : `export $(grep -v '^#' conf/env | sed 's/[[:space:]]*$//' | xargs)` ou `set -a && source conf/env && set +a` pour que les variable d'environnement qui se trouvent le répertoire conf du projet soient sourcées.

* Si l'on veut changer de Location  des données il faut supprimer les tables puis les récréer.

```sql
-- Dans Trino CLI
DROP TABLE IF EXISTS hive.ecommerce.products;
DROP TABLE IF EXISTS hive.ecommerce.customers;
DROP TABLE IF EXISTS hive.ecommerce.orders;
DROP SCHEMA IF EXISTS hive.ecommerce;
```


