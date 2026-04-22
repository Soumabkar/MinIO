package minio

import config.AppConfig._
import entity.DataGenerator._
import datawarehouse.MinIOLoader._
import sqlengine.TrinoClient._
import spark.SparkProcessor._
import org.slf4j.LoggerFactory

object Main extends App {
  val log = LoggerFactory.getLogger(getClass)

  log.info("=" * 60)
  log.info("  Démarrage du Pipeline Lakehouse (Scala)")
  log.info("=" * 60)

  val cfg = AppConfig.fromEnv()

  // ── 1. Génération ─────────────────────────────────────────
  log.info("\n[1/5] Génération des données...")
  val spark     = new SparkProcessor(cfg)
  val generator = new DataGenerator(spark.spark)

  val customersDf = generator.generateCustomers(500)
  val productsDf  = generator.generateProducts(100)
  val ordersDf    = generator.generateOrders(2000)

  log.info(s"  Clients  : ${customersDf.count()} lignes")
  log.info(s"  Produits : ${productsDf.count()} lignes")
  log.info(s"  Commandes: ${ordersDf.count()} lignes")

  // ── 2. MinIO ──────────────────────────────────────────────
  log.info("\n[2/5] Chargement dans MinIO...")
  val loader = new MinIOLoader(cfg)
  loader.uploadDataFrame(customersDf, "customers")
  loader.uploadDataFrame(productsDf,  "products")
  loader.uploadDataFrame(ordersDf,    "orders", partitionCols = Seq("year", "month"))
  log.info(s"  ${loader.listObjects().length} objets dans MinIO")

  // ── 3. Hive / Trino ───────────────────────────────────────
  log.info("\n[3/5] Création du schéma et des tables Hive...")
  val trino = new TrinoClient(cfg)
  trino.createSchema()
  trino.createTables(cfg.minioBucket)
  trino.syncPartitions(ordersDf, cfg.minioBucket)

  // ── 4. Spark KPIs ─────────────────────────────────────────
  log.info("\n[4/5] Calculs PySpark...")
  val ordersS    = spark.readParquet("orders")
  val customersS = spark.readParquet("customers")
  val productsS  = spark.readParquet("products")

  log.info(s"  Orders   : ${ordersS.count()} lignes")
  log.info(s"  Customers: ${customersS.count()} lignes")
  log.info(s"  Products : ${productsS.count()} lignes")

  spark.runKpis(ordersS, customersS, productsS)
  spark.stop()

  // ── 5. Trino analytics ────────────────────────────────────
  log.info("\n[5/5] Requêtes analytiques Trino...")
  trino.runAnalytics()

  log.info("\nPipeline terminé avec succès !")
}