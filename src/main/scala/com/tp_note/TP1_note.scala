package com.tp_note
// Louise STOSCHEK

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object TP1_note {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    import org.apache.spark.sql.functions._

    // Exercice 1

    // Question 1
    val rdd = sparkSession.sparkContext.textFile("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\scala-spark-boilerplate\\data\\donnees.csv")


    // Question 2
    val film_diCaprio = rdd.filter(elem => elem.contains("Di Caprio"))
    //println(film_diCaprio.count())



    // Question 3
    val note_film = film_diCaprio.map(elem => elem.split(";")(2).toDouble)



    // Question 4


    // Question 5


    // Exercice 2

    // Question 1
    val df: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\scala-spark-boilerplate\\data\\donnees.csv")

    // Question 2

    //val col_film = df.columns.map(elem => elem.replace(col("_c0"), "nom_film"))
    //col_film.foreach(println)
    val col_c0_rename = df.withColumnRenamed("_c0", "nom_film")
    val col_c1_rename = col_c0_rename.withColumnRenamed("_c1", "nombre_vues")
    val col_c2_rename = col_c1_rename.withColumnRenamed("_c2", "note_film")
    val df_col_rename = col_c2_rename.withColumnRenamed("_c3", "acteur_principal")
    //df_col_rename.printSchema()


    // Question 3

    val film_diCaprio_ex2 = df_col_rename.filter(col("acteur_principal") contains("Di Caprio"))
    //println(film_diCaprio_ex2.count())

    val note_moy_ex2 = df_col_rename.groupBy("acteur_principal").avg("note_film")
    val note_moy_diCaprio_ex2 = note_moy_ex2.filter(col("acteur_principal") contains("Di Caprio"))
    //note_moy_diCaprio_ex2.show()

    val pourcentage_tot_leo = film_diCaprio_ex2.groupBy("acteur_principal").agg(sum("nombre_vues").as("Sum"))
    val test = film_diCaprio_ex2.withColumn("pourcentage_vue_leo", col("note_film") * 100 / 8063229227)
    test.show()


    // Question 4


  }
}
