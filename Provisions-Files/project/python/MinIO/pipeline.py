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

import io
import os
import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import trino
from faker import Faker
from minio import Minio
from minio.error import S3Error
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ( DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,)

# ─────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",    "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "abdoul1201")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",      "warehouse")

TRINO_HOST       = os.getenv("TRINO_HOST",         "localhost")
TRINO_PORT       = int(os.getenv("TRINO_PORT",     "8080"))
TRINO_USER       = os.getenv("TRINO_USER",         "admin")
TRINO_CATALOG    = "hive"
TRINO_SCHEMA     = "ecommerce"

SPARK_MASTER     = os.getenv("SPARK_MASTER",       "local[*]")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)
fake = Faker("fr_FR")


# ═════════════════════════════════════════════════════════
#  PARTIE 1 – Génération des données
# ═════════════════════════════════════════════════════════

def generate_customers(n: int = 500) -> pd.DataFrame:
    """Génère un DataFrame de clients fictifs."""
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "customer_id":  i,
            "first_name":   fake.first_name(),
            "last_name":    fake.last_name(),
            "email":        fake.email(),
            "country":      fake.country_code(representation="alpha-2"),
            "city":         fake.city(),
            "created_at":   fake.date_time_between(start_date="-2y", end_date="now"),
        })
    df = pd.DataFrame(rows)
    df["created_at"] = pd.to_datetime(df["created_at"])
    return df


def generate_products(n: int = 100) -> pd.DataFrame:
    """Génère un catalogue de produits."""
    categories = ["Electronics", "Clothing", "Food", "Books", "Sports", "Home"]
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "product_id":   i,
            "name":         fake.catch_phrase(),
            "category":     fake.random_element(categories),
            "price":        round(fake.pyfloat(min_value=5, max_value=500), 2),
            "stock":        fake.random_int(min=0, max=1000),
        })
    return pd.DataFrame(rows)


def generate_orders(
    n: int = 2000,
    max_customer_id: int = 500,
    max_product_id: int = 100,
) -> pd.DataFrame:
    """Génère des commandes avec partitionnement par année/mois."""
    statuses = ["COMPLETED", "PENDING", "CANCELLED", "REFUNDED"]
    rows = []
    base_date = datetime.now() - timedelta(days=365 * 2)
    for i in range(1, n + 1):
        order_date = fake.date_time_between(start_date=base_date, end_date="now")
        rows.append({
            "order_id":     i,
            "customer_id":  fake.random_int(min=1, max=max_customer_id),
            "product_id":   fake.random_int(min=1, max=max_product_id),
            "quantity":     fake.random_int(min=1, max=10),
            "unit_price":   round(fake.pyfloat(min_value=5, max_value=500), 2),
            "status":       fake.random_element(statuses),
            "order_date":   order_date,
            "year":         order_date.year,
            "month":        order_date.month,
        })
    df = pd.DataFrame(rows)
    df["total_amount"] = df["quantity"] * df["unit_price"]
    df["order_date"] = pd.to_datetime(df["order_date"])
    return df


# ═════════════════════════════════════════════════════════
#  PARTIE 2 – Chargement dans MinIO
# ═════════════════════════════════════════════════════════

class MinIOLoader:
    """Gère l'upload de DataFrames Pandas vers MinIO en format Parquet."""

    def __init__(self):
        self.client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        self._ensure_bucket(MINIO_BUCKET)

    def _ensure_bucket(self, bucket: str) -> None:
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)
            log.info(f"Bucket '{bucket}' créé.")
        else:
            log.info(f"Bucket '{bucket}' existant.")

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        object_path: str,
        partition_cols: Optional[list] = None,
    ) -> None:
        """
        Upload un DataFrame en Parquet vers MinIO.
        Si partition_cols est spécifié, écrit en partitions Hive-style.
        Ex: s3://lakehouse/warehouse/orders/year=2024/month=1/data.parquet
        """
        if partition_cols:
            for keys, group in df.groupby(partition_cols):
                # Construire le chemin de partition
                if not isinstance(keys, tuple):
                    keys = (keys,)
                parts = "/".join(
                    f"{col}={val}"
                    for col, val in zip(partition_cols, keys)
                )
                path = f"{object_path}/{parts}/data.parquet"
                self._upload_to_minio(group.drop(columns=partition_cols), path)
        else:
            self._upload_to_minio(df, f"{object_path}/data.parquet")

    def _upload_to_minio(self, df: pd.DataFrame, object_name: str) -> None:
        table = pa.Table.from_pandas(df, preserve_index=False)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        size = buf.getbuffer().nbytes
        self.client.put_object(
            MINIO_BUCKET,
            object_name,
            buf,
            size,
            content_type="application/octet-stream",
        )
        log.info(f"  ✅ Uploadé : s3://{MINIO_BUCKET}/{object_name} ({size/1024:.1f} KB)")

    def list_objects(self, prefix: str = "") -> list:
        objects = self.client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]


# ═════════════════════════════════════════════════════════
#  PARTIE 3 – Création du schéma et des tables via Trino
# ═════════════════════════════════════════════════════════

class TrinoManager:
    """Gère les connexions et DDL/DML via Trino."""

    def __init__(self):
        self.conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA,
        )

    def execute(self, sql: str, fetch: bool = False):
        cursor = self.conn.cursor()
        log.info(f"SQL → {sql[:80]}...")
        cursor.execute(sql)
        if fetch:
            return cursor.fetchall(), [desc[0] for desc in cursor.description]
        return cursor

    def create_schema(self) -> None:
        s3_location = f"s3a://{MINIO_BUCKET}/warehouse"
        self.execute(f"""
            CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}
            WITH (location = '{s3_location}')
        """)
        log.info(f"Schéma '{TRINO_SCHEMA}' prêt.")

    def create_table_customers(self) -> None:
        self.execute(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.customers (
                customer_id  INTEGER,
                first_name   VARCHAR,
                last_name    VARCHAR,
                email        VARCHAR,
                country      VARCHAR,
                city         VARCHAR,
                created_at   TIMESTAMP
            )
            WITH (
                format           = 'PARQUET',
                external_location = 's3a://{MINIO_BUCKET}/warehouse/customers'
            )
        """)
        log.info("Table 'customers' créée.")

    def create_table_products(self) -> None:
        self.execute(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.products (
                product_id  INTEGER,
                name        VARCHAR,
                category    VARCHAR,
                price       DOUBLE,
                stock       INTEGER
            )
            WITH (
                format           = 'PARQUET',
                external_location = 's3a://{MINIO_BUCKET}/warehouse/products'
            )
        """)
        log.info("Table 'products' créée.")

    def create_table_orders(self) -> None:
        """Table partitionnée par année et mois."""
        self.execute(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.orders (
                order_id      INTEGER,
                customer_id   INTEGER,
                product_id    INTEGER,
                quantity      INTEGER,
                unit_price    DOUBLE,
                total_amount  DOUBLE,
                status        VARCHAR,
                order_date    TIMESTAMP,
                year          INTEGER,
                month         INTEGER
            )
            WITH (
                format             = 'PARQUET',
                external_location  = 's3a://{MINIO_BUCKET}/warehouse/orders',
                partitioned_by     = ARRAY['year', 'month']
            )
        """)
        log.info("Table 'orders' (partitionnée year/month) créée.")

    def sync_partitions(self) -> None:
        """Synchronise les partitions détectées depuis MinIO."""
        self.execute(f"CALL {TRINO_CATALOG}.system.sync_partition_metadata('{TRINO_SCHEMA}', 'orders', 'FULL')")
        log.info("Partitions synchronisées.")

    def run_analytics(self) -> None:
        """Exemples de requêtes analytiques Trino."""
        queries = {
            "Chiffre d'affaires par pays (Top 10)": f"""
                SELECT
                    c.country,
                    COUNT(DISTINCT o.order_id)          AS nb_commandes,
                    ROUND(SUM(o.total_amount), 2)        AS ca_total,
                    ROUND(AVG(o.total_amount), 2)        AS panier_moyen
                FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.orders o
                JOIN {TRINO_CATALOG}.{TRINO_SCHEMA}.customers c
                  ON o.customer_id = c.customer_id
                WHERE o.status = 'COMPLETED'
                GROUP BY c.country
                ORDER BY ca_total DESC
                LIMIT 10
            """,
            "Top 5 catégories par revenus": f"""
                SELECT
                    p.category,
                    COUNT(o.order_id)             AS nb_ventes,
                    ROUND(SUM(o.total_amount), 2) AS revenu
                FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.orders o
                JOIN {TRINO_CATALOG}.{TRINO_SCHEMA}.products p
                  ON o.product_id = p.product_id
                WHERE o.status != 'CANCELLED'
                GROUP BY p.category
                ORDER BY revenu DESC
                LIMIT 5
            """,
            "Évolution mensuelle des ventes (2024)": f"""
                SELECT
                    year,
                    month,
                    COUNT(*)                          AS nb_commandes,
                    ROUND(SUM(total_amount), 2)       AS ca,
                    ROUND(AVG(total_amount), 2)       AS panier_moyen
                FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.orders
                WHERE year = 2024
                  AND status = 'COMPLETED'
                GROUP BY year, month
                ORDER BY year, month
            """,
            "Clients les plus dépensiers (Top 5)": f"""
                SELECT
                    c.customer_id,
                    c.first_name || ' ' || c.last_name AS client,
                    c.country,
                    COUNT(o.order_id)              AS nb_commandes,
                    ROUND(SUM(o.total_amount), 2)  AS depenses_totales
                FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.orders o
                JOIN {TRINO_CATALOG}.{TRINO_SCHEMA}.customers c
                  ON o.customer_id = c.customer_id
                WHERE o.status = 'COMPLETED'
                GROUP BY c.customer_id, c.first_name, c.last_name, c.country
                ORDER BY depenses_totales DESC
                LIMIT 5
            """,
        }

        print("\n" + "═" * 60)
        print("  📊  RÉSULTATS ANALYTIQUES TRINO")
        print("═" * 60)

        for title, sql in queries.items():
            rows, cols = self.execute(sql.strip(), fetch=True)
            df = pd.DataFrame(rows, columns=cols)
            print(f"\n▶  {title}")
            print(df.to_string(index=False))
            print()


# ═════════════════════════════════════════════════════════
#  PARTIE 4 – Lecture et transformations avec PySpark
# ═════════════════════════════════════════════════════════

class SparkProcessor:
    """Lecture depuis MinIO/Hive et transformations PySpark."""

    def __init__(self):
        self.spark = (
            SparkSession.builder
            .master(SPARK_MASTER)
            .appName("Lakehouse-Pipeline")
            .config("spark.hadoop.fs.s3a.endpoint",          f"http://{MINIO_ENDPOINT}")
            .config("spark.hadoop.fs.s3a.access.key",         MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key",         MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access",  "true")
            .config("spark.hadoop.fs.s3a.impl",               "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.adaptive.enabled",              "true")
            .config("spark.sql.shuffle.partitions",            "20")
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            )
            .enableHiveSupport()
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")
        log.info("SparkSession initialisée.")

    def read_parquet(self, path: str):
        """Lit un répertoire Parquet partitionné depuis MinIO."""
        full_path = f"s3a://{MINIO_BUCKET}/{path}"
        log.info(f"Lecture Spark : {full_path}")
        return self.spark.read.parquet(full_path)

    def read_hive_table(self, table: str):
        """Lit une table depuis le Hive Metastore."""
        return self.spark.table(f"{TRINO_SCHEMA}.{table}")

    def compute_kpis(self, orders_df, customers_df, products_df) -> None:
        """Calcule des KPIs métier avec PySpark."""

        # 1. Revenu par catégorie
        revenue_by_category = (
            orders_df
            .join(products_df, "product_id")
            .filter(F.col("status") == "COMPLETED")
            .groupBy("category")
            .agg(
                F.count("order_id").alias("nb_commandes"),
                F.round(F.sum("total_amount"), 2).alias("revenu_total"),
                F.round(F.avg("total_amount"), 2).alias("panier_moyen"),
            )
            .orderBy(F.desc("revenu_total"))
        )

        print("\n" + "─" * 50)
        print("  🔥  KPIs PySpark – Revenu par catégorie")
        print("─" * 50)
        revenue_by_category.show(truncate=False)

        # 2. Taux d'annulation par pays
        cancellation_rate = (
            orders_df
            .join(customers_df, "customer_id")
            .groupBy("country")
            .agg(
                F.count("order_id").alias("total"),
                F.sum(
                    F.when(F.col("status") == "CANCELLED", 1).otherwise(0)
                ).alias("annulations"),
            )
            .withColumn(
                "taux_annulation",
                F.round(F.col("annulations") / F.col("total") * 100, 1),
            )
            .filter(F.col("total") > 10)
            .orderBy(F.desc("taux_annulation"))
        )

        print("\n  📉  Taux d'annulation par pays (>10 commandes)")
        cancellation_rate.show(10, truncate=False)

        # 3. Fenêtre glissante – CA cumulé mensuel
        monthly_sales = (
            orders_df
            .filter(F.col("status") == "COMPLETED")
            .groupBy("year", "month")
            .agg(F.round(F.sum("total_amount"), 2).alias("ca_mensuel"))
            .withColumn(
                "ca_cumule",
                F.round(
                    F.sum("ca_mensuel").over(
                        __import__("pyspark.sql.window", fromlist=["Window"])
                        .Window.orderBy("year", "month")
                        .rowsBetween(
                            __import__("pyspark.sql.window", fromlist=["Window"])
                            .Window.unboundedPreceding, 0
                        )
                    ),
                    2,
                ),
            )
            .orderBy("year", "month")
        )

        print("\n  📈  CA mensuel + cumulé")
        monthly_sales.show(24, truncate=False)

    def stop(self) -> None:
        self.spark.stop()
        log.info("SparkSession arrêtée.")


# ═════════════════════════════════════════════════════════
#  PIPELINE PRINCIPAL
# ═════════════════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info("  🚀  Démarrage du Pipeline Lakehouse")
    log.info("=" * 60)

    # ── 1. Génération des données ──────────────────────────
    log.info("\n📦  [1/5] Génération des données...")
    customers_df = generate_customers(500)
    products_df  = generate_products(100)
    orders_df    = generate_orders(2000)
    log.info(f"  Clients  : {len(customers_df):>5} lignes")
    log.info(f"  Produits : {len(products_df):>5} lignes")
    log.info(f"  Commandes: {len(orders_df):>5} lignes")

    # ── 2. Chargement MinIO ────────────────────────────────
    log.info("\n☁️   [2/5] Chargement dans MinIO...")
    loader = MinIOLoader()
    loader.upload_dataframe(customers_df, "warehouse/customers")
    loader.upload_dataframe(products_df,  "warehouse/products")
    loader.upload_dataframe(
        orders_df,
        "warehouse/orders",
        partition_cols=["year", "month"],
    )

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
    trino_mgr.sync_partitions()

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
