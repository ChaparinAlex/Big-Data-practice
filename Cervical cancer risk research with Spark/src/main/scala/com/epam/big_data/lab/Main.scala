package com.epam.big_data.lab

import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]) {
    val sparkContext = new SparkContext(new SparkConf().setAppName("Spark Cancer Risk Research").setMaster("local"))
    val csvOperations = new CSVOperations(sparkContext)
    csvOperations.writeData()
  }

}
