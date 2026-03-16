val rawDf = TimedStage("lecture S3 raw") {
  S3Reader.readRaw(spark, "s3a://bucket/raw/", "parquet")
}

val cleanDf = TimedStage("nettoyage + enrichissement") {
  Transformer.cleanAndEnrich(rawDf)
}

val aggDf = TimedStage("agrégation mensuelle") {
  Transformer.aggregate(cleanDf)
}

TimedStage("écriture Iceberg MERGE") {
  IcebergWriter.upsert(spark, aggDf, "dwh.transactions_monthly")
}

m.report()
IcebergMetrics.logScanInfo(spark, "dwh.transactions_monthly")