package minio.entity

import com.github.javafaker.Faker
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import scala.util.Random

class DataGenerator(spark: SparkSession) {
  private val faker  = new Faker()
  private val random = new Random()

  private val categories = Seq("Electronics", "Clothing", "Food", "Books", "Sports", "Home")
  private val statuses   = Seq("COMPLETED", "PENDING", "CANCELLED", "SHIPPED")

  // ── Customers ────────────────────────────────────────────
  def generateCustomers(n: Int = 500): DataFrame = {
    val schema = StructType(Seq(
      StructField("customer_id", LongType,   nullable = false),
      StructField("first_name",  StringType, nullable = true),
      StructField("last_name",   StringType, nullable = true),
      StructField("email",       StringType, nullable = true),
      StructField("country",     StringType, nullable = true),
      StructField("created_at",  TimestampType, nullable = true),
    ))

    val rows = (1 to n).map { i =>
      Row(
        i.toLong,
        faker.name().firstName(),
        faker.name().lastName(),
        faker.internet().emailAddress(),
        faker.address().countryCode(),
        Timestamp.valueOf(
          LocalDateTime.now().minusDays(random.nextInt(730))
        ),
      )
    }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  // ── Products ─────────────────────────────────────────────
  def generateProducts(n: Int = 100): DataFrame = {
    val schema = StructType(Seq(
      StructField("product_id", LongType,   nullable = false),
      StructField("name",       StringType, nullable = true),
      StructField("category",   StringType, nullable = true),
      StructField("price",      DoubleType, nullable = true),
      StructField("stock",      IntegerType, nullable = true),
    ))

    val rows = (1 to n).map { i =>
      Row(
        i.toLong,
        faker.commerce().productName(),
        categories(random.nextInt(categories.length)),
        Math.round((10 + random.nextDouble() * 990) * 100.0) / 100.0,
        random.nextInt(500),
      )
    }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  // ── Orders ───────────────────────────────────────────────
  def generateOrders(n: Int = 2000, customerCount: Int = 500, productCount: Int = 100): DataFrame = {
    val schema = StructType(Seq(
      StructField("order_id",     LongType,   nullable = false),
      StructField("customer_id",  LongType,   nullable = true),
      StructField("product_id",   LongType,   nullable = true),
      StructField("quantity",     IntegerType, nullable = true),
      StructField("unit_price",   DoubleType, nullable = true),
      StructField("status",       StringType, nullable = true),
      StructField("order_date",   TimestampType, nullable = true),
      StructField("total_amount", DoubleType, nullable = true),
      StructField("year",         IntegerType, nullable = true),
      StructField("month",        IntegerType, nullable = true),
    ))

    val start = LocalDateTime.of(2024, 3, 1, 0, 0)
    val end   = LocalDateTime.now()
    val days  = ChronoUnit.DAYS.between(start, end).toInt

    val rows = (1 to n).map { i =>
      val qty       = 1 + random.nextInt(10)
      val price     = Math.round((10 + random.nextDouble() * 990) * 100.0) / 100.0
      val orderDate = start.plusDays(random.nextInt(days))
        .plusHours(random.nextInt(24))
      Row(
        i.toLong,
        (1 + random.nextInt(customerCount)).toLong,
        (1 + random.nextInt(productCount)).toLong,
        qty,
        price,
        statuses(random.nextInt(statuses.length)),
        Timestamp.valueOf(orderDate),
        Math.round(qty * price * 100.0) / 100.0,
        orderDate.getYear,
        orderDate.getMonthValue,
      )
    }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }
}