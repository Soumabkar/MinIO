package writer

import org.apache.spark.sql.{DataFrame, SparkSession}

object IcebergWriter {

  // Créer la table si elle n'existe pas, puis append
  def createOrAppend(spark: SparkSession, df: DataFrame, tableName: String): Unit = {
    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS iceberg.$tableName (
        year        INT,
        month       INT,
        country     STRING,
        total_eur   DOUBLE,
        unique_users LONG
      )
      USING iceberg
      PARTITIONED BY (year, month)
      LOCATION 's3a://mon-bucket/warehouse/$tableName'
    """)

    df.writeTo(s"iceberg.$tableName").append()
  }

  // Upsert (MERGE INTO) : mise à jour incrémentale sans doublons
  def upsert(spark: SparkSession, df: DataFrame, tableName: String): Unit = {
    df.createOrReplaceTempView("incoming")
    spark.sql(s"""
      MERGE INTO iceberg.$tableName AS target
      USING incoming AS source
        ON target.year = source.year
       AND target.month = source.month
       AND target.country = source.country
      WHEN MATCHED THEN UPDATE SET
        total_eur    = source.total_eur,
        unique_users = source.unique_users
      WHEN NOT MATCHED THEN INSERT *
    """)
  }

  // Overwrite d'une partition spécifique (remplacement idempotent)
  def overwritePartition(df: DataFrame, tableName: String, year: Int, month: Int): Unit =
    df.writeTo(s"iceberg.$tableName")
      .overwritePartitions()
}