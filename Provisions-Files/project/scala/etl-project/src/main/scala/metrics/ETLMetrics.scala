package metrics

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

case class ETLMetrics(spark: SparkSession) {

  val rowsRead      : LongAccumulator = spark.sparkContext.longAccumulator("rows_read")
  val rowsRejected  : LongAccumulator = spark.sparkContext.longAccumulator("rows_rejected")
  val rowsWritten   : LongAccumulator = spark.sparkContext.longAccumulator("rows_written")
  val filesOpened   : LongAccumulator = spark.sparkContext.longAccumulator("s3_files_opened")

  def report(): Unit = {
    println(s"""
      |--- Métriques ETL ---
      | Lignes lues     : ${rowsRead.value}
      | Lignes rejetées : ${rowsRejected.value}  (${pct(rowsRejected.value, rowsRead.value)}%)
      | Lignes écrites  : ${rowsWritten.value}
      | Fichiers S3     : ${filesOpened.value}
      """.stripMargin)
  }

  private def pct(part: Long, total: Long): String =
    if (total == 0) "0" else f"${part * 100.0 / total}%.1f"
}