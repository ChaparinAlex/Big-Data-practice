package com.epam.big_data.lab

import org.apache.spark.SparkContext

object Main {

  def main(args: Array[String]) {
    val sparkContext = new SparkContext()
    val csvOperations = new CSVOperations(sparkContext)
    csvOperations.writeData()
  }

}
