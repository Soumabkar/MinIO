import trino
import logging
import pandas as pd
from utils.env import env, env_int


TRINO_HOST       = env("TRINO_HOST")
TRINO_PORT       = env_int("TRINO_PORT")
TRINO_USER       = env("TRINO_USER")
TRINO_CATALOG    = env("TRINO_CATALOG")
TRINO_SCHEMA     = env("TRINO_SCHEMA")
MINIO_BUCKET     = env("MINIO_BUCKET")
DATA_FOLDER      = env("DATA_FOLDER")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

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

    def create_schema(self, bucket: str = MINIO_BUCKET, data_folder: str = DATA_FOLDER, catalog: str = TRINO_CATALOG, schema: str = TRINO_SCHEMA) -> None:
        s3_location = f"s3a://{bucket}/{data_folder}"
        self.execute(f"""
            CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
            WITH (location = '{s3_location}')
        """)
        log.info(f"Schéma '{schema}' prêt.")

    def create_table_customers(self ,  bucket: str = MINIO_BUCKET, data_folder: str = DATA_FOLDER, catalog: str = TRINO_CATALOG, schema: str = TRINO_SCHEMA , table_name: str = "customers") -> None:
        self.execute(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} (
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
                external_location = 's3a://{bucket}/{data_folder}/{table_name}'
            )
        """)
        log.info(f"Table '{table_name}' créée.")

    def create_table_products(self, bucket: str = MINIO_BUCKET, data_folder: str = DATA_FOLDER, catalog: str = TRINO_CATALOG, schema: str = TRINO_SCHEMA, table_name: str = "products") -> None:
        self.execute(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} (
                product_id  INTEGER,
                name        VARCHAR,
                category    VARCHAR,
                price       DOUBLE,
                stock       INTEGER
            )
            WITH (
                format           = 'PARQUET',
                external_location = 's3a://{bucket}/{data_folder}/{table_name}'
            )
        """)
        log.info(f"Table '{table_name}' créée.")

    def create_table_orders(self, bucket: str = MINIO_BUCKET, data_folder: str = DATA_FOLDER, catalog: str = TRINO_CATALOG, schema: str = TRINO_SCHEMA, table_name: str = "orders") -> None:
        """Table partitionnée par année et mois."""
        self.execute(f"""
            CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} (
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
                external_location  = 's3a://{bucket}/{data_folder}/{table_name}',
                partitioned_by     = ARRAY['year', 'month']
            )
        """)
        log.info(f"Table '{table_name}' (partitionnée year/month) créée.")

    def sync_partitions(self ,table_name: str, mode: str = "ADD_ONLY") -> None:
        try:
            if(mode == "ADD_ONLY" ):
                self.execute(f"CALL {TRINO_CATALOG}.system.sync_partition_metadata('{TRINO_SCHEMA}', '{table_name}', 'ADD_ONLY')") 
            if(mode == "FULL" ):
                self.execute(f"CALL {TRINO_CATALOG}.system.sync_partition_metadata('{TRINO_SCHEMA}', '{table_name}', 'FULL')")
        except Exception as e:
                log.warning(f"sync_partition_metadata({mode}) échoué : {e}")
        log.info(f"Partitions synchronisées avec le mode : {mode}")


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
