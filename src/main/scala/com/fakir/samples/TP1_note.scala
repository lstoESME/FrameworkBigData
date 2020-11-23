package com.fakir.samples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TP1_note {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //val df: DataFrame = sparkSession.read.option("delimiter",",").option("inferschema",true).option("header", true).csv(path = "films.csv")

    // Question 1
    val rdd = sparkSession.sparkContext.textFile("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\scala-spark-boilerplate\\data\\dataOct-22-2020.csv")
    println(rdd) //compte nombre de lignes
    val rdd_sans_M = rdd.filter(elem => !elem.startsWith("M"))
    val rdd_nb_enfant = rdd_sans_M.map(elem => elem.split(";")(2))
    val rdd_nb_plus2enf = rdd_nb_enfant.filter(elem => elem > "21")
    val somme = rdd_nb_plus2enf
    rdd_nb_plus2enf.foreach(println)


  }
}