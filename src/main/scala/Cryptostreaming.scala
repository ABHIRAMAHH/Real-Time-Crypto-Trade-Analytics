import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object CryptoStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Crypto Streaming")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "crypto_trades")
      .option("startingOffsets", "earliest")
      .load()

    val schema = StructType(Array(
      StructField("trade_id", StringType),
      StructField("coin", StringType),
      StructField("exchange", StringType),
      StructField("price", DoubleType),
      StructField("volume", IntegerType),
      StructField("trade_type", StringType),
      StructField("trader_region", StringType),
      StructField("fee", DoubleType),
      StructField("trade_time", StringType)
    ))

    val parsedDF = kafkaDF
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")
      .withColumn(
        "trade_time",
        to_timestamp(col("trade_time"))
      )

      .withColumn(
        "trade_value",
        col("price") * col("volume")
      )

      .withColumn(
        "net_trade_value",
        col("price") * col("volume") - col("fee")
      )

      .withColumn(
        "price_category",
        when(col("price") < 20000, "Low")
          .when(col("price") < 70000, "Medium")
          .otherwise("High")
      )

      .withColumn(
        "volume_category",
        when(col("volume") < 30, "Small")
          .when(col("volume") < 70, "Medium")
          .otherwise("Large")
      )

      .withColumn(
        "hour",
        hour(col("trade_time"))
      )

      .withColumn(
        "trade_day",
        date_format(col("trade_time"), "yyyy-MM-dd")
      )

    val query = parsedDF.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) =>

        batchDF.write
          .format("jdbc")
          .option(
            "url",
            "jdbc:postgresql://localhost:5432/streaming_db"
          )
          .option("dbtable", "processed_trades")
          .option("user", "admin")
          .option("password", "admin123")
          .option(
            "driver",
            "org.postgresql.Driver"
          )
          .mode("append")
          .save()

        println(s"Batch $batchId written to PostgreSQL")
      }
      .start()

    query.awaitTermination()
  }
}