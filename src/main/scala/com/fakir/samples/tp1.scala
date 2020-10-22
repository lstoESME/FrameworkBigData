package com.fakir.samples

import org.apache.spark.sql.SQLContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object tp1 {

  def majuscule(s: String, filtre: String): String = {
    if(s.contains(filtre)) s
    else s.toUpperCase
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    // Question 1
    val rdd = sparkSession.sparkContext.textFile("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\scala-spark-boilerplate\\data\\dataOct-22-2020.csv")
    println(rdd.count()) //compte nombre de lignes
    val rdd_sans_M = rdd.filter(elem => !elem.startsWith("M"))
    val rdd_nb_enfant = rdd_sans_M.map(elem => elem.split(";")(1))
    val rdd_nb_plus2enf = rdd_nb_enfant.filter(elem => elem > "2")
    rdd_nb_plus2enf.foreach(println)
    println(rdd_nb_enfant.count())

    /* Correction
    val rdd1 = sparkSession.sparkContext.textFile("data/user.csv")
//rdd1.foreach(println)
val suprdd = rdd1.filter(elem => !elem.contains("E") || elem.split(";")(1).toDouble > 2)
suprdd.foreach(println)
println(suprdd.count())
val counts = suprdd.map(item => (item.split(";")(2).toDouble, (1.0, item.split(";")(1).toDouble)) )
val countSums = counts.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))
val keyMeans = countSums.mapValues(avgCount => avgCount._2 / avgCount._1)
keyMeans.foreach(println)
     */
  }
}
