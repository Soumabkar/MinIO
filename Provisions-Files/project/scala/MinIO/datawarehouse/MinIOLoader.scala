package com.lakehouse.datawarehouse

import com.lakehouse.config.AppConfig
import io.minio.{BucketExistsArgs, MakeBucketArgs, PutObjectArgs, MinioClient, ListObjectsArgs}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream

class MinIOLoader(cfg: AppConfig) {
  private val log = LoggerFactory.getLogger(getClass)

  private val client = MinioClient.builder()
    .endpoint(s"http://${cfg.minioEndpoint}")
    .credentials(cfg.minioAccessKey, cfg.minioSecretKey)
    .build()

  ensureBucket(cfg.minioBucket)

  private def ensureBucket(bucket: String): Unit = {
    val exists = client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build())
    if (!exists) {
      client.makeBucket(MakeBucketArgs.builder().bucket(bucket).build())
      log.info(s"Bucket '$bucket' créé.")
    } else {
      log.info(s"Bucket '$bucket' existant.")
    }
  }

  /**
   * Upload un DataFrame Spark en Parquet partitionné vers MinIO via l'API S3A.
   * Spark écrit directement sur s3a:// — pas besoin du client MinIO pour l'upload.
   */
  def uploadDataFrame(
    df             : DataFrame,
    tableName      : String,
    partitionCols  : Seq[String] = Seq.empty,
  ): Unit = {
    val path = s"s3a://${cfg.minioBucket}/${cfg.dataFolder}/$tableName"

    val writer = df.write
      .mode(SaveMode.Overwrite)
      .option("compression", "snappy")

    if (partitionCols.nonEmpty)
      writer.partitionBy(partitionCols: _*).parquet(path)
    else
      writer.parquet(path)

    log.info(s"  ✅ Uploadé : $path")
  }

  def listObjects(prefix: String = ""): List[String] = {
    import scala.jdk.CollectionConverters._
    val args = ListObjectsArgs.builder()
      .bucket(cfg.minioBucket)
      .prefix(prefix)
      .recursive(true)
      .build()
    client.listObjects(args).asScala
      .map(_.get().objectName())
      .toList
  }
}