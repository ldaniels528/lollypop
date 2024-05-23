package com.lollypop.runtime.spark351

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object StockQuotesConverter {
  def main(args: Array[String]): Unit = {
    // create a SparkSession
    val spark = SparkSession
      .builder()
      .appName("StockQuotesConverter")
      .config("spark.master", "local[*]")
      .getOrCreate()

    // import spark implicits for DataFrame operations
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // read some stock quotes
    val quotesDF: DataFrame = spark.read
      .option("header", "true")
      .schema(StructType(Array(
        StructField("symbol", StringType, nullable = true),
        StructField("exchange", StringType, nullable = true),
        StructField("lastSale", DoubleType, nullable = true),
        StructField("lastSaleDateTime", TimestampType, nullable = true),
      )))
      .csv("./app/examples/stocks-100k.csv")
      .withColumn("lastSaleDate", to_date($"lastSaleDateTime"))
      .withColumn("lastSaleTime", date_format($"lastSaleDateTime", "HH:mm:ss.SSSZ"))

    // write the stock quotes to JSON files
    quotesDF//.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("exchange")
      //.bucketBy(5, "exchange")
      .json("stock_quotes_output")

    // stop the SparkSession
    spark.stop()
  }
}
