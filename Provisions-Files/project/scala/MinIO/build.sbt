ThisBuild / version      := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.lakehouse"

lazy val root = project.in(file(".")).settings(
  name := "lakehouse-pipeline",

  libraryDependencies ++= Seq(
    // Spark
    "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
    "org.apache.spark" %% "spark-sql"  % "3.5.0" % "provided",

    // MinIO / S3
    "io.minio"          % "minio"              % "8.5.7",
    "org.apache.hadoop" % "hadoop-aws"         % "3.3.4",
    "com.amazonaws"     % "aws-java-sdk-bundle" % "1.12.262",

    // Trino
    "io.trino" % "trino-jdbc" % "435",

    // Faker
    "com.github.javafaker" % "javafaker" % "1.0.2",

    // Config
    "com.typesafe" % "config" % "1.4.3",

    // Logging
    "org.slf4j"      % "slf4j-api"       % "2.0.9",
    "ch.qos.logback" % "logback-classic" % "1.4.14",
  ),

  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case "reference.conf"              => MergeStrategy.concat
    case _                             => MergeStrategy.first
  },
)