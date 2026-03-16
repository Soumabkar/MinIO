package ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}

object S3Reader {

  // Lecture de fichiers bruts (CSV, JSON, Parquet) depuis la zone raw S3
  def readRaw(spark: SparkSession, path: String, format: String = "parquet"): DataFrame =
    spark.read
      .format(format)
      .option("header", "true")   // pour CSV
      .option("inferSchema", "true")
      .load(path)

  // Lecture directe d'une table Iceberg existante
  def readIcebergTable(spark: SparkSession, tableName: String): DataFrame =
    spark.table(s"iceberg.$tableName")

  // Time travel : lire un snapshot précédent
  def readIcebergSnapshot(spark: SparkSession, tableName: String, snapshotId: Long): DataFrame =
    spark.read
      .format("iceberg")
      .option("snapshot-id", snapshotId)
      .load(s"iceberg.$tableName")
}