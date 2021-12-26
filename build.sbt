name := "spark-streaming-hw"

version := "0.1"

scalaVersion := "2.12.15"

idePackagePrefix := Some("com.example.spark_streaming_hw")

val sparkVersion = "3.1.2"

lazy val kafka_producer = project
  .settings(
    libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.8.0"
  )

lazy val spark_app = project
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
    )
  )

lazy val root = (project in file("."))
  .aggregate(kafka_producer, spark_app)
