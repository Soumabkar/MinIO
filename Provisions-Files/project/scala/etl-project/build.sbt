val sparkVersion  = "3.5.1"
val icebergVersion = "1.5.2"

libraryDependencies ++= Seq(
  "org.apache.spark"   %% "spark-sql"           % sparkVersion % "provided",
  "org.apache.iceberg"  % "iceberg-spark-runtime-3.5_2.12" % icebergVersion,
  "org.apache.hadoop"   % "hadoop-aws"           % "3.3.6",
  "software.amazon.awssdk" % "bundle"            % "2.25.0"
)