package transform

import org.apache.spark.sql.{DataFrame, functions => F}

object Transformer {

  def cleanAndEnrich(df: DataFrame): DataFrame =
    df
      .filter(F.col("event_date").isNotNull)
      .withColumn("year",  F.year(F.col("event_date")))
      .withColumn("month", F.month(F.col("event_date")))
      .withColumn("amount_eur",
        F.col("amount_usd") * F.lit(0.92))
      .dropDuplicates("transaction_id")

  def aggregate(df: DataFrame): DataFrame =
    df.groupBy("year", "month", "country")
      .agg(
        F.sum("amount_eur").alias("total_eur"),
        F.countDistinct("user_id").alias("unique_users")
      )
}