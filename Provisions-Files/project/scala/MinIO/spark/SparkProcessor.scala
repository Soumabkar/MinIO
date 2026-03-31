package com.lakehouse.spark

import com.lakehouse.config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.slf4j.LoggerFactory

class SparkProcessor(cfg: AppConfig) {
  private val log = LoggerFactory.getLogger(getClass)

  val spark: SparkSession = SparkSession.builder()
    .appName("Lakehouse Pipeline")
    .master(cfg.sparkMaster)
    .config("spark.hadoop.fs.s3a.endpoint",               s"http://${cfg.minioEndpoint}")
    .config("spark.hadoop.fs.s3a.access.key",             cfg.minioAccessKey)
    .config("spark.hadoop.fs.s3a.secret.key",             cfg.minioSecretKey)
    .config("spark.hadoop.fs.s3a.path.style.access",      "true")
    .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.parquet.outputTimestampType",       "TIMESTAMP_MICROS")
    .config("spark.sql.parquet.int96RebaseModeInRead",     "CORRECTED")
    .config("spark.sql.parquet.datetimeRebaseModeInRead",  "CORRECTED")
    .config("spark.jars.packages",
      "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  log.info("SparkSession initialisée.")

  def readParquet(tableName: String): DataFrame = {
    val path = s"s3a://${cfg.minioBucket}/${cfg.dataFolder}/$tableName"
    log.info(s"Lecture Spark : $path")
    spark.read.parquet(path)
  }

  def runKpis(orders: DataFrame, customers: DataFrame, products: DataFrame): Unit = {

    // 1. Revenu par catégorie
    println("\n" + "─" * 50)
    println("  KPIs PySpark – Revenu par catégorie")
    println("─" * 50)

    orders
      .filter(col("status") === "COMPLETED")
      .join(products, "product_id")
      .groupBy("category")
      .agg(
        count("order_id").alias("nb_commandes"),
        round(sum("total_amount"), 2).alias("revenu_total"),
        round(avg("total_amount"), 2).alias("panier_moyen"),
      )
      .orderBy(desc("revenu_total"))
      .show(truncate = false)

    // 2. Taux d'annulation par pays
    println("  Taux d'annulation par pays (>10 commandes)")

    val byCountry = orders
      .join(customers, "customer_id")
      .groupBy("country")
      .agg(
        count("order_id").alias("total"),
        count(when(col("status") === "CANCELLED", 1)).alias("annulations"),
      )
      .filter(col("total") > 10)
      .withColumn(
        "taux_annulation",
        round(col("annulations") * 100.0 / col("total"), 1),
      )
      .orderBy(desc("taux_annulation"))

    byCountry.show(10, truncate = false)

    // 3. CA mensuel + cumulé
    println("  CA mensuel + cumulé")

    val wCumul = Window.orderBy("year", "month")
      .rowsBetween(Window.unboundedPreceding, 0)

    orders
      .filter(col("status") === "COMPLETED")
      .groupBy("year", "month")
      .agg(round(sum("total_amount"), 2).alias("ca_mensuel"))
      .withColumn("ca_cumule", round(sum("ca_mensuel").over(wCumul), 2))
      .orderBy("year", "month")
      .show(30, truncate = false)
  }

  def stop(): Unit = {
    spark.stop()
    log.info("SparkSession arrêtée.")
  }
}