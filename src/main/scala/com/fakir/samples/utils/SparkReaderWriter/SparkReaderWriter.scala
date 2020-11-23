package com.fakir.samples.utils.SparkReaderWriter

import org.apache.hadoop.mapred.InputFormat
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkReaderWriter {
  def readData(inputPath: String, inputFormat: String): DataFrame = {
    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    if (inputFormat == "CVS")
      sparkSession.read.csv(inputPath)
    else {
      sparkSession.read.parquet(inputPath)
    }
  }

  def writeData(df: DataFrame, outputPath: String, outputFormat: String, partitions: Seq[String]) = {
    if(outputFormat == "CSV") {
      df.write.partitionBy(partitions:_*).csv(outputPath)
    }
    else {
      df.write.partitionBy(partitions:_*).parquet(outputPath)
    }
  }
}
