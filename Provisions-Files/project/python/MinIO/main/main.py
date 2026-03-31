"""
=============================================================
  Lakehouse Pipeline – MinIO + Hive + Trino + Spark
=============================================================
  Étapes :
    1. Génération de données fictives (Faker)
    2. Chargement des fichiers Parquet dans MinIO
    3. Création du schéma et des tables Hive via Trino
    4. Lecture des données avec PySpark
    5. Requêtes analytiques avec Trino
=============================================================
"""

import logging
from faker import Faker
from Entity.Data import Customers, Products, Orders
from Datawarehouse.Minio import MinIOLoader
from SqlEngine.Trino import TrinoManager
from Spark.SparkMinIo import SparkProcessor
from utils.env import env, env_int


# ─────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────
MINIO_ENDPOINT   = env("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = env("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = env("MINIO_SECRET_KEY")
MINIO_BUCKET     = env("MINIO_BUCKET")

TRINO_HOST       = env("TRINO_HOST")
TRINO_PORT       = env_int("TRINO_PORT")
TRINO_USER       = env("TRINO_USER")
TRINO_CATALOG    = "hive"
TRINO_SCHEMA     = "ecommerce"

SPARK_MASTER     = env("SPARK_MASTER")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)
fake = Faker("fr_FR")

# ═════════════════════════════════════════════════════════
#  PIPELINE PRINCIPAL
# ═════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("  🚀  Démarrage du Pipeline Lakehouse")
    log.info("=" * 60)

    # ── 1. Génération des données ──────────────────────────
    log.info("\n📦  [1/5] Génération des données...")
    customers = Customers()
    products  = Products()
    orders    = Orders()
    customers_df = customers.generate_customers(500)
    products_df  = products.generate_products(100)
    orders_df    = orders.generate_orders(2000)
    log.info(f"  Clients  : {len(customers_df):>5} lignes")
    log.info(f"  Produits : {len(products_df):>5} lignes")
    log.info(f"  Commandes: {len(orders_df):>5} lignes")

    # ── 2. Chargement MinIO ────────────────────────────────
    log.info("\n☁️   [2/5] Chargement dans MinIO...")
    loader = MinIOLoader()
    loader.upload_dataframe(customers_df, "warehouse/customers")
    loader.upload_dataframe(products_df,  "warehouse/products")
    loader.upload_dataframe(orders_df,  "warehouse/orders", partition_cols=["year", "month"], )

        # Liste des objets uploadés
    objects = loader.list_objects("warehouse/")
    log.info(f"  {len(objects)} objets dans MinIO/warehouse")

    # ── 3. Création schéma + tables Hive via Trino ─────────
    log.info("\n🗃️   [3/5] Création du schéma et des tables Hive...")
    trino_mgr = TrinoManager()
    trino_mgr.create_schema()
    trino_mgr.create_table_customers()
    trino_mgr.create_table_products()
    trino_mgr.create_table_orders()
    trino_mgr.sync_partitions("FULL")

    # ── 4. Lecture et KPIs PySpark ─────────────────────────
    log.info("\n⚡  [4/5] Lecture et calculs PySpark...")
    spark_proc = SparkProcessor()
    try:
        orders_spark   = spark_proc.read_parquet("warehouse/orders")
        customers_spark = spark_proc.read_parquet("warehouse/customers")
        products_spark  = spark_proc.read_parquet("warehouse/products")

        log.info(f"  Orders  : {orders_spark.count():>6} lignes | Partitions: {orders_spark.rdd.getNumPartitions()}")
        log.info(f"  Customers: {customers_spark.count():>5} lignes")
        log.info(f"  Products : {products_spark.count():>5} lignes")

        orders_spark.printSchema()
        spark_proc.compute_kpis(orders_spark, customers_spark, products_spark)
    finally:
        spark_proc.stop()

    # ── 5. Requêtes analytiques Trino ─────────────────────
    log.info("\n🔍  [5/5] Requêtes analytiques Trino...")
    trino_mgr.run_analytics()

    log.info("\n✅  Pipeline terminé avec succès !")

   

if __name__ == "__main__":
    main()
