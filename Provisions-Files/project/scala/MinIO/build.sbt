ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "minio"

// 👉 Important pour Spark + Java 17
ThisBuild / fork := true

ThisBuild / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "-Djava.net.preferIPv4Stack=true"
)

lazy val root = project
  .in(file("."))
  .settings(
    name := "lakehouse-pipeline",

    // ⚠️ à éviter sauf besoin spécifique
    // Compile / scalaSource := baseDirectory.value,

    libraryDependencies ++= Seq(

      // 🔹 Spark
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql"  % "3.5.0",

      // 🔹 MinIO / S3
      "io.minio"          % "minio"               % "8.5.7",
      "org.apache.hadoop" % "hadoop-aws"          % "3.3.4",
      "com.amazonaws"     % "aws-java-sdk-bundle" % "1.12.262",

      // 🔹 Trino
      "io.trino" % "trino-jdbc" % "435",

      // 🔹 Faker
      "com.github.javafaker" % "javafaker" % "1.0.2",

      // 🔹 Config
      "com.typesafe" % "config" % "1.4.3",

      // 🔹 Logging (UNE seule implémentation)
      "org.slf4j"      % "slf4j-api"       % "2.0.9",
      "ch.qos.logback" % "logback-classic" % "1.4.14"
    ),

    // ✅ Nettoyage conflits logs (très important avec Spark)
    libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) },

    // ✅ Fix classloader sbt
    run / fork := true,

    // ✅ Assembly
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf"              => MergeStrategy.concat
      case _                             => MergeStrategy.first
    }
  )