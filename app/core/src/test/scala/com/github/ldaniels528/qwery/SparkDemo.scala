package com.github.ldaniels528.qwery

import org.slf4j.LoggerFactory

object SparkDemo {

  def main(args: Array[String]): Unit = {
    // instantiate the SLF4J logger
    val logger = LoggerFactory.getLogger("SparkDemo")
/*
    import org.apache.spark.sql.{SaveMode, SparkSession}

    // init the the Spark session
    logger.info("Initializing Spark context...")
    val sqlContext = SparkSession.builder()
      .appName("SparkDemo")
      .master("local[*]")
      .getOrCreate()

    // read the company list as a dataframe
    val companyListCSV = sqlContext.read
      .option("header", "true")
      .csv("./demos/companylist/csv/")

    // write the company list out in JSON format
    logger.info("Copying data...")
    companyListCSV.show(5)
    companyListCSV.write.mode(SaveMode.Overwrite).json("./temp/companyList/json/")
    logger.info("Done")
*/
  }

}
