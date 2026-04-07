# Projet Datawaeahouse

## Prérequis et installation

Pour le bon fonctionnement de notre plateform nous allons installer `vagrant` https://developer.hashicorp.com/vagrant et `virtualbox` https://www.virtualbox.org/.
`vagrant` va nous permettre de manager nos mochine virtuelle sur `virtualbox`.
Nous allons installer cela grace à `chocolatey` https://chocolatey.org/

### Installation Chocolatey

* Ouvre PowerShell en tant qu'administrateur et exécute cette commande :
    ```powershell
    Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
    ```

* Vérifier que `chocolatey` est bien installé via la commande :
    `choco --version`

### Installation des outils

* Installation virtualbox

    Ouvre PowerShell en tant qu'administrateur et exécute :
    ```powershell
    choco install virtualbox -y
    ```
    Avec options utiles :
    ```powershell
    # Installer une version spécifique
    choco install virtualbox --version=7.0.14 -y

    # Installer VirtualBox + Extension Pack en même temps
    choco install virtualbox virtualbox-guest-additions-guest.install -y

    # Vérifier les versions disponibles
    choco list virtualbox --all-versions
    ```
* Après installation :

    ```powershell
    # Vérifier que c'est bien installé
    choco list --local-only | findstr virtualbox

    # Mettre à jour plus tard
    choco upgrade virtualbox -y
    ```

## Mise en place de la plateform

Pour mettre en place la plateform, il faut se rendre à la racine du projet où se trouve le fichier Vagrantfile, puis executez la commande `vagrant up` pour démarrer la VM et faire les installation automatiquement si c'est votre prémier fois de lancer. 
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

## Architecture

```
┌─────────────────────────────────────────────────────┐
│              VM Vagrant (Ubuntu 22.04)              │
│                  IP: 192.168.56.10                  │
│                  RAM: 8Go | CPU: 2                  │
│                                                     │
│  ┌──────────────────────────────────────────────┐   │
│  │         Docker (datalake-network)            │   │
│  │                                              │   │
│  │  ┌─────────┐         ┌──────────────────┐    │   │
│  │  │  MinIO  │◄────────│  hive-metastore  │    │   │
│  │  │ :9000   │         │     :9083        │    │   │
│  │  │ :9001   │         └────────┬─────────┘    │   │
│  │  └─────────┘                  │              │   │
│  │                      ┌────────▼─────────┐    │   │
│  │  ┌─────────┐         │  metastore-db    │    │   │
│  │  │  Trino  │─────────│  (PostgreSQL)    │    │   │
│  │  │  :8080  │         │     :5432        │    │   │
│  │  └─────────┘         └──────────────────┘    │   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

### Le Vagrantfile — La VM hôte

C'est la fondation : il crée une machine virtuelle VirtualBox qui hébergera tous les conteneurs Docker.

```ruby
config.vm.box = "ubuntu/jammy64"   # Ubuntu 22.04 LTS
vb.memory = "8192"                 # 8 Go RAM pour Spark/Trino
vb.cpus   = 2
config.vm.network "private_network", ip: "192.168.56.10"
```

Il copie aussi les fichiers Python du projet dans la VM via config.vm.provision "file" :

* Minio.py → client MinIO
* SparkMinIo.py → intégration Spark
* Trino.py → moteur SQL
* main.py  → orchestration (ELT)


### MinIO — Le stockage (compatible S3)
```yaml
ports:
  - "9000:9000"   # API S3
  - "9001:9001"   # Console web
environment:
  MINIO_DOMAIN: minio
```
C'est le lac de données : il stocke les fichiers (Parquet, CSV, JSON...) sous forme de buckets, exactement comme AWS S3. Les autres services (Hive, Trino, Spark) lisent et écrivent leurs données ici via l'API S3.

### metastore-db (PostgreSQL) — La base du catalogue

```yaml
environment:
  POSTGRES_DB: metastore_db
  POSTGRES_USER: hive
```
C'est la base de données relationnelle qui stocke les métadonnées du catalogue Hive : noms des tables, schémas, chemins S3, types de colonnes, partitions... Elle ne contient pas les données elles-mêmes, seulement leur description.

###  hive-metastore — Le catalogue

```yaml
depends_on:
  metastore-db:   # condition: service_healthy
  minio:          # condition: service_healthy
ports:
  - "9083:9083"   # Thrift API
environment:
  SERVICE_NAME: metastore
  SKIP_SCHEMA_INIT: "false"   # initialise le schéma au 1er démarrage
```
C'est le catalogue central : il fait le lien entre les tables logiques (noms, schémas) stockées dans PostgreSQL et les fichiers physiques stockés dans MinIO. Il expose une API Thrift sur le port 9083 que Trino consulte pour savoir "où sont les données de la table X ?".


### Trino — Le moteur SQL

```yaml
depends_on:
  hive-metastore:   # condition: service_healthy
ports:
  - "8080:8080"
volumes:
  - ./trino-config:/etc/trino:ro
```

### Le flux de données complet

```
Requête SQL (utilisateur)
        │
        ▼
     Trino :8080
        │
        ├──► Hive Metastore :9083  ──► PostgreSQL :5432
        │         "où est la table ?"      (métadonnées)
        │
        └──► MinIO :9000
                  "donne-moi les fichiers Parquet"
                  (données réelles)
```

Ordre de démarrage garanti par `depends_on`
```
PostgreSQL  →  hive-metastore  →  Trino
MinIO       ↗
```
Les `healthcheck` sur chaque service assurent que le suivant ne démarre qu'une fois le précédent vraiment prêt, pas juste lancé.

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


