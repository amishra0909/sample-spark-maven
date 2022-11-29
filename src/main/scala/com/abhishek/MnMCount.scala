package com.abhishek

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Usage: MnMCount <mnm_file_dataset>
 */
object MnMCount {
  def main(args:Array[String]) {
    val spark = SparkSession
      .builder
      .appName("MnMCount")
      .getOrCreate()

    if(args.length < 1) {
      print("Usage: MnMCount <mnm_file_dataset>")
      sys.exit(1)
    }

    // Get MnM dataset filename
    val mnmFile = args(0);

    // Read the file into spark dataframe
    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferschema", "true")
      .load(mnmFile)

    mnmDF.show(10)

    // Aggregate the count of all colors and groupby() State and Color
    // OrderBy() in descending order
    val countMnMDF = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))

    // Show resulting aggregations for all states and colors
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()

    // Find the aggregate counts for California by filtering
    val caMnMCountDF = mnmDF
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))

    // Show the resulting aggregations for California
    caMnMCountDF.show(10)
    
    // Stop the SparkSession
    spark.stop()
  }
}

