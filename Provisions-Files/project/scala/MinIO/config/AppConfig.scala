package minio.config

case class AppConfig(
  minioEndpoint  : String,
  minioAccessKey : String,
  minioSecretKey : String,
  minioBucket    : String,
  dataFolder     : String,
  trinoHost      : String,
  trinoPort      : Int,
  trinoUser      : String,
  trinoCatalog   : String,
  trinoSchema    : String,
  sparkMaster    : String,
)

object AppConfig {
  def fromEnv(): AppConfig = {
    def env(key: String, default: String = ""): String =
      sys.env.getOrElse(key, default).strip()

    AppConfig(
      minioEndpoint   = env("MINIO_ENDPOINT"),
      minioAccessKey  = env("MINIO_ACCESS_KEY"),
      minioSecretKey  = env("MINIO_SECRET_KEY"),
      minioBucket     = env("MINIO_BUCKET"),
      dataFolder      = env("DATA_FOLDER", "datawarehouse"),
      trinoHost       = env("TRINO_HOST", "localhost"),
      trinoPort       = env("TRINO_PORT", "8080").toInt,
      trinoUser       = env("TRINO_USER", "admin"),
      trinoCatalog    = env("TRINO_CATALOG", "hive"),
      trinoSchema     = env("TRINO_SCHEMA", "ecommerce"),
      sparkMaster     = env("SPARK_MASTER", "local[*]"),
    )
  }
}