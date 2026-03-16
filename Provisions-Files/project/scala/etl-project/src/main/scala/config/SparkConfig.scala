package config

import org.apache.spark.sql.SparkSession

object SparkConfig {

  def createSession(appName: String): SparkSession = SparkSession.builder()
    .appName(appName)
    // --- Iceberg catalog (REST ou Hive Metastore) ---
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type",   "rest")   // ou "hive"
    .config("spark.sql.catalog.iceberg.uri",    "http://iceberg-rest:8181")
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://mon-bucket/warehouse/")
    // --- Accès S3 via hadoop-aws ---
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.path.style.access", "false")
    // --- Performances ---
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
}