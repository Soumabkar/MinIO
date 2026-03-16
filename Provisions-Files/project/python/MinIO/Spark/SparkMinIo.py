from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
import os

SPARK_MASTER     = os.getenv("SPARK_MASTER")

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)



MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET")

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
