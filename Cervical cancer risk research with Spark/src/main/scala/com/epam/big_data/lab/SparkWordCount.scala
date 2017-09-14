package com.epam.big_data.lab

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkWordCount {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"))

    // read in text file and split each document into words
    val tokenized = sc.textFile("src\\main\\resources\\words.txt").flatMap(_.split(","))

    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)

    System.out.println(wordCounts.collect().mkString(", "))
  }
}
