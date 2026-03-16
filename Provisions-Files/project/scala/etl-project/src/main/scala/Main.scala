import config.SparkConfig
import ingestion.S3Reader
import transform.Transformer
import writer.IcebergWriter

object Main extends App {

  val spark = SparkConfig.createSession("ETL-Iceberg-Pipeline")

  // 1. Lecture raw depuis S3
  val rawDf = S3Reader.readRaw(
    spark,
    path   = "s3a://mon-bucket/raw/transactions/date=2024-01-*/",
    format = "parquet"
  )

  // 2. Transformation
  val cleanDf = Transformer.cleanAndEnrich(rawDf)
  val aggDf   = Transformer.aggregate(cleanDf)

  // 3. Écriture / mise à jour dans Iceberg
  IcebergWriter.upsert(spark, aggDf, "dwh.transactions_monthly")

  spark.stop()
}