package com.lakehouse.sqlengine

import com.lakehouse.config.AppConfig
import org.slf4j.LoggerFactory
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

class TrinoClient(cfg: AppConfig) {
  private val log = LoggerFactory.getLogger(getClass)

  Class.forName("io.trino.jdbc.TrinoDriver")

  private def connection(): Connection = {
    val url   = s"jdbc:trino://${cfg.trinoHost}:${cfg.trinoPort}/${cfg.trinoCatalog}/${cfg.trinoSchema}"
    val props = new Properties()
    props.setProperty("user", cfg.trinoUser)
    DriverManager.getConnection(url, props)
  }

  def execute(sql: String): Unit = {
    val shortSql = sql.trim.take(80).replace('\n', ' ')
    log.info(s"SQL → $shortSql...")
    val conn = connection()
    try {
      val stmt = conn.createStatement()
      stmt.execute(sql)
      stmt.close()
    } finally {
      conn.close()
    }
  }

  def query(sql: String): ResultSet = {
    val conn = connection()
    val stmt = conn.createStatement()
    stmt.executeQuery(sql)
  }

  // ── DDL ──────────────────────────────────────────────────
  def createSchema(): Unit = {
    execute(s"""
      CREATE SCHEMA IF NOT EXISTS ${cfg.trinoCatalog}.${cfg.trinoSchema}
      WITH (location = 's3a://${???}/${cfg.dataFolder}/')
    """)
    log.info(s"Schéma '${cfg.trinoSchema}' prêt.")
  }

  def createTables(bucket: String): Unit = {
    val base = s"s3a://$bucket/${cfg.dataFolder}"
    val cat  = cfg.trinoCatalog
    val sch  = cfg.trinoSchema

    execute(s"""
      CREATE TABLE IF NOT EXISTS $cat.$sch.customers (
        customer_id BIGINT,
        first_name  VARCHAR,
        last_name   VARCHAR,
        email       VARCHAR,
        country     VARCHAR,
        created_at  TIMESTAMP
      ) WITH (
        external_location = '$base/customers',
        format = 'PARQUET'
      )
    """)
    log.info("Table 'customers' créée.")

    execute(s"""
      CREATE TABLE IF NOT EXISTS $cat.$sch.products (
        product_id BIGINT,
        name       VARCHAR,
        category   VARCHAR,
        price      DOUBLE,
        stock      INTEGER
      ) WITH (
        external_location = '$base/products',
        format = 'PARQUET'
      )
    """)
    log.info("Table 'products' créée.")

    execute(s"""
      CREATE TABLE IF NOT EXISTS $cat.$sch.orders (
        order_id     BIGINT,
        customer_id  BIGINT,
        product_id   BIGINT,
        quantity     INTEGER,
        unit_price   DOUBLE,
        status       VARCHAR,
        order_date   TIMESTAMP,
        total_amount DOUBLE,
        year         INTEGER,
        month        INTEGER
      ) WITH (
        external_location = '$base/orders',
        format            = 'PARQUET',
        partitioned_by    = ARRAY['year', 'month']
      )
    """)
    log.info("Table 'orders' (partitionnée year/month) créée.")
  }

  def syncPartitions(df: org.apache.spark.sql.DataFrame, bucket: String): Unit = {
    val base = s"s3a://$bucket/${cfg.dataFolder}/orders"
    val cat  = cfg.trinoCatalog
    val sch  = cfg.trinoSchema

    // Tentative FULL d'abord
    try {
      execute(s"CALL $cat.system.sync_partition_metadata('$sch', 'orders', 'FULL')")
      log.info("Partitions synchronisées (FULL).")
      return
    } catch {
      case e: Exception =>
        log.warn(s"sync_partition_metadata(FULL) échoué : ${e.getMessage}")
    }

    // Fallback : ALTER TABLE ADD PARTITION par partition
    import org.apache.spark.sql.functions._
    val partitions = df.select("year", "month").distinct().collect()
    var ok = 0; var ko = 0
    partitions.foreach { row =>
      val year  = row.getInt(0)
      val month = row.getInt(1)
      try {
        execute(s"""
          ALTER TABLE $cat.$sch.orders
          ADD IF NOT EXISTS PARTITION (year=$year, month=$month)
          LOCATION '$base/year=$year/month=$month/'
        """)
        ok += 1
      } catch {
        case e: Exception =>
          log.warn(s"  Partition $year/$month ignorée : ${e.getMessage}")
          ko += 1
      }
    }
    log.info(s"  Partitions : $ok ajoutées, $ko ignorées.")
  }

  // ── Requêtes analytiques ──────────────────────────────────
  def runAnalytics(): Unit = {
    val cat = cfg.trinoCatalog
    val sch = cfg.trinoSchema

    val queries = Map(
      "CA par pays (Top 10)" ->
        s"""SELECT c.country,
           |       COUNT(DISTINCT o.order_id)          AS nb_commandes,
           |       ROUND(SUM(o.total_amount), 2)        AS ca_total,
           |       ROUND(AVG(o.total_amount), 2)        AS panier_moyen
           |FROM $cat.$sch.orders o
           |JOIN $cat.$sch.customers c ON o.customer_id = c.customer_id
           |WHERE o.status = 'COMPLETED'
           |GROUP BY c.country
           |ORDER BY ca_total DESC
           |LIMIT 10""".stripMargin,

      "Top 5 catégories" ->
        s"""SELECT p.category,
           |       COUNT(o.order_id)             AS nb_ventes,
           |       ROUND(SUM(o.total_amount), 2) AS revenu
           |FROM $cat.$sch.orders o
           |JOIN $cat.$sch.products p ON o.product_id = p.product_id
           |WHERE o.status = 'COMPLETED'
           |GROUP BY p.category
           |ORDER BY revenu DESC
           |LIMIT 5""".stripMargin,

      "Évolution mensuelle 2024" ->
        s"""SELECT year, month,
           |       COUNT(*)                       AS nb_commandes,
           |       ROUND(SUM(total_amount), 2)    AS ca,
           |       ROUND(AVG(total_amount), 2)    AS panier_moyen
           |FROM $cat.$sch.orders
           |WHERE status = 'COMPLETED' AND year = 2024
           |GROUP BY year, month
           |ORDER BY month""".stripMargin,

      "Clients top dépensiers (Top 5)" ->
        s"""SELECT c.customer_id,
           |       c.first_name || ' ' || c.last_name  AS client,
           |       c.country,
           |       COUNT(o.order_id)                    AS nb_commandes,
           |       ROUND(SUM(o.total_amount), 2)        AS depenses_totales
           |FROM $cat.$sch.orders o
           |JOIN $cat.$sch.customers c ON o.customer_id = c.customer_id
           |WHERE o.status = 'COMPLETED'
           |GROUP BY c.customer_id, c.first_name, c.last_name, c.country
           |ORDER BY depenses_totales DESC
           |LIMIT 5""".stripMargin,
    )

    println("\n" + "=" * 60)
    println("  RÉSULTATS ANALYTIQUES TRINO")
    println("=" * 60)

    queries.foreach { case (title, sql) =>
      println(s"\n▶  $title")
      val rs = query(sql)
      val md = rs.getMetaData
      val cols = (1 to md.getColumnCount).map(md.getColumnName)
      println(cols.mkString(" | "))
      println("-" * 60)
      while (rs.next()) {
        val row = cols.map(c => Option(rs.getString(c)).getOrElse("null"))
        println(row.mkString(" | "))
      }
      rs.getStatement.getConnection.close()
    }
  }
}