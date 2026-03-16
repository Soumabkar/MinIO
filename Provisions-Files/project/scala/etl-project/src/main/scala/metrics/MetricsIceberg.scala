package metrics

import org.apache.spark.sql.SparkSession

object IcebergMetrics {

  def logScanInfo(spark: SparkSession, tableName: String): Unit = {
    // Fichiers dans la table + taille totale
    val files = spark.sql(s"SELECT * FROM iceberg.$tableName.files")
    files.groupBy("content")
         .agg(
           org.apache.spark.sql.functions.count("*").as("nb_files"),
           org.apache.spark.sql.functions.sum("file_size_in_bytes").as("total_bytes")
         )
         .show()

    // Historique des snapshots (commits)
    spark.sql(s"""
      SELECT snapshot_id, committed_at, operation,
             summary['added-files-count']    AS files_added,
             summary['deleted-files-count']  AS files_deleted,
             summary['added-records']        AS records_added
      FROM iceberg.$tableName.snapshots
      ORDER BY committed_at DESC
      LIMIT 5
    """).show(truncate = false)
  }
}
```

---

## 5. Export Prometheus — métriques temps réel

Ajoutez dans `spark-defaults.conf` ou directement dans `SparkConfig` :
```
spark.metrics.conf.*.sink.prometheus.class=
  org.apache.spark.metrics.sink.PrometheusSink
spark.metrics.conf.*.sink.prometheus.pushgateway-address=
  http://pushgateway:9091
spark.metrics.conf.*.sink.prometheus.pushgateway-enable-timestamp=true

# Métriques JVM (heap, GC, threads)
spark.metrics.conf.*.source.jvm.class=
  org.apache.spark.metrics.source.JvmSource