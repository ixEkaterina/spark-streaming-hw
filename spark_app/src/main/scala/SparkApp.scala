import org.apache.spark.sql.functions.{col, from_json, window}
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

// https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
object SparkApp extends App {

  val spark = SparkSession.builder
    .appName("spark-streaming-hw")
    .master(sys.env.getOrElse("spark.master", "local[*]"))
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val brokers = "localhost:9092"
  val topic = "records"

  val records: DataFrame = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", brokers)
    .option("subscribe", topic)
    .load()

  import spark.implicits._
  val dfString = records.withColumn("json", $"value".cast("String"))

  val scheme = new StructType()
    .add("bedroom", DoubleType)
    .add("kitchen", DoubleType)
    .add("bathroom", DoubleType)

  val dfJSON = records.withColumn("jsonData",from_json(col("value").cast("String"),scheme))
    .select("timestamp", "jsonData.*")

  val windowedCounts = dfJSON
    .groupBy(
      window($"timestamp", "10 seconds", "10 seconds"))
    .avg("bedroom", "kitchen", "bathroom")

  val query = windowedCounts.writeStream
    .outputMode("update")
    .format("console")
    .start()

  query.awaitTermination()

}
