error id: file:///D:/training/editions_eni/MinIO/Provisions-Files/project/scala/etl-project/src/main/scala/metrics/UseMetricsInMain.scala:cleanAndEnrich.
file:///D:/training/editions_eni/MinIO/Provisions-Files/project/scala/etl-project/src/main/scala/metrics/UseMetricsInMain.scala
empty definition using pc, found symbol in pc: cleanAndEnrich.
semanticdb not found
empty definition using fallback
non-local guesses:
	 -cleanAndEnrich.
	 -cleanAndEnrich#
	 -cleanAndEnrich().
	 -scala/Predef.cleanAndEnrich.
	 -scala/Predef.cleanAndEnrich#
	 -scala/Predef.cleanAndEnrich().
offset: 188
uri: file:///D:/training/editions_eni/MinIO/Provisions-Files/project/scala/etl-project/src/main/scala/metrics/UseMetricsInMain.scala
text:
```scala
val rawDf = TimedStage("lecture S3 raw") {
  S3Reader.readRaw(spark, "s3a://bucket/raw/", "parquet")
}

val cleanDf = TimedStage("nettoyage + enrichissement") {
  Transformer.cleanAnd@@Enrich(rawDf)
}

val aggDf = TimedStage("agrégation mensuelle") {
  Transformer.aggregate(cleanDf)
}

TimedStage("écriture Iceberg MERGE") {
  IcebergWriter.upsert(spark, aggDf, "dwh.transactions_monthly")
}

m.report()
IcebergMetrics.logScanInfo(spark, "dwh.transactions_monthly")
```


#### Short summary: 

empty definition using pc, found symbol in pc: cleanAndEnrich.