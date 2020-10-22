package com.fakir.samples

import org.apache.spark.sql.SQLContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object tp1 {
  Logger.getLogger("org").setLevel(Level.OFF)

  val sparkSession = SparkSession.builder().master("local").getOrCreate()

  val rdd = sparkSession.sparkContext.textFile("data")
  rdd.foreach(println)
}
