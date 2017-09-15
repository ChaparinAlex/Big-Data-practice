package com.epam.big_data.lab

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

class CSVOperations{

  var sparkContext: SparkContext = null
  var sqlContext: SQLContext = null

  def this(value : SparkContext) = {
    this();
    this.sparkContext = value;
    sqlContext = new SQLContext(this.sparkContext)
  }

  def getDataFromCSV() : DataFrame = {
    return sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("src\\main\\resources\\input_data.csv");
  }

  def getSelectedData() : DataFrame = {
    return getDataFromCSV().select("Age", "Smokes")
  }

  def writeData(): Unit ={
    getSelectedData().write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("src\\main\\resources\\output\\output_data.csv")
  }


}
