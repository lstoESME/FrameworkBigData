package com.fakir.samples

import java.text.SimpleDateFormat
import java.util.Date

import com.fakir.samples.config.ConfigParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


object tp2Dataframe {




  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //val df: DataFrame = sparkSession.read.option("header", true).csv(path = "data.csv")
/* Lit la df en analysant les types :*/
    val df: DataFrame = sparkSession.read.option("delimiter",",").option("inferschema",true).option("header", true).csv(path = "data.csv")
    //df.printSchema()
    import org.apache.spark.sql.functions._
    /*Sélectionner selon les colonnes
    val selectedDF = df.select("Region", "Country")
    import org.apache.spark.sql.functions._
    /*Affiche les colonnes REGION et COUNTRY*/
    selectedDF.filter(col("COUNTRY") === "Chad").show
    /*Affiche tous les produits dont le pays est le Chad*/
    df.filter(col("COUNTRY") === "Chad").show
    */

    /*Création nouvelle colonne Units_Sold_Plus_1
    import org.apache.spark.sql.functions._
    val result = df.withColumn("Units_Sold_Plus_1", col("Units Sold") + 1)
    /*Création nouvelle colonne à la base d'une colonne existente.
    df.filter(col("COUNTRY") === "Chad")
    val result = df.withColumn("Units_Sold_Plus_Unit_Price", col("Units Sold")
      + col("Units Price"))*/
    result.show()
    */

    /*
    import sparkSession.implicits._ //doit se mettre obligatoirement dans le main
    val stringValues = df.select("Country").as[String].collect()
    //stringValues.foreach(println)
    stringValues.distinct.foreach(println)
    */

    /*Affiche noms des colonnes.
    df.columns.foreach(println)
    */

    /*Remplace les espaces par des "_" dans le nom des colonnes.
    val columnsWithoutSpaces = df.columns.map(elem => elem.replaceAll(" ", "_"))
    //columnsWithoutSpaces.foreach(println)

    //méthode toDF : renomme toutes les col du DF avec une liste donnée en paramètre.
    val dfWithRightColumnsName = df.toDF(columnsWithoutSpaces:_*)
    dfWithRightColumnsName.show()
    dfWithRightColumnsName.write.parquet("result")
    cf question 10*/

    /*UDF*/
    val transformField = udf((date: String) => {
      val formatDate = new SimpleDateFormat("yyyy-MM-dd")
      formatDate.format(new Date(date))
    })
    val transformedDateDf = df.withColumn("Order Date", transformField(col("Order Date")))



    /* Dataframes Exercices*/
    /*2 : combien de produits ayant un prix unitaire supérieur à 500 et plus de 3000 unités vendues ?*/
    val prix_unit_500 = df.filter(col("Unit Price") > 500)
    val unites_vendues = prix_unit_500.filter(col("Units Sold") > 3000)
    //println(unites_vendues.count())

    /*3 : Faites la somme de tous les produits vendus valant plus de $500 en prix unitaire*/
    val somme = df.filter(col("Unit Price") > 500).groupBy("Unit Price").sum("Units Sold")
    //somme.show()

    /*4 : Quel est le prix moyen de tous les produits vendus ? (En groupant par item_type)*/
    val prix_moy = df.groupBy("Item Type").avg("Unit Price")
    //prix_moy.show()

    /*5: Créer une nouvelle colonne, "total_revenue" contenant le revenu total des ventes par produit vendu.*/
    val tot_revenue = df.withColumn("total_revenue", col("Units Sold") * col("Unit Price"))
    //tot_revenue.show()

    /*6: Créer une nouvelle colonne, "total_cost", contenant le coût total des ventes par produit vendu.*/
    val cost = tot_revenue.withColumn("total_cost", col("Units Sold") * col("Unit Cost"))
    //cost.show()

    /*7: Créer une nouvelle colonne, "total_profit", contenant le bénéfice réalisé par produit*/
    val profit = cost.withColumn("total_profit", col("Total Revenue") - col("Total Cost"))
    //profit.show()


    /*8: Créer une nouvelle colonne, "unit_price_discount", qui aura comme valeur "unit_price" si le nombre d'unités vendues est plus de 3000. Le montant du discount doit être de 20%.*/
    val discount = profit.withColumn("unit_price_discount", when(col("Units Sold") > 3000, col("Unit Price") * 0.7).otherwise(col("Unit Price") * 0.9))
    //discount.show

    /*9: Faire répercuter ça sur les 3 colonnes créées précédemment : total_revenue, total_cost et total_profit.*/
    val df1 = discount.withColumn("total_revenue", col("Units Sold") * col("unit_price_discount"))
    //df1.show()
    val df2 = df1.withColumn("total_profit", when(col("Units Sold") > 3000, col("Units Sold") * (col("Unit Price") - col("Unit Cost"))))
    //df2.show()

    /*10: Écrire le résultat sous format parquet, partitionné par formatted_ship_date*/
    val columnsList = df.columns
    val columnsWithtoutSpaces: Array[String] = columnsList.map(elem => elem.replaceAll(" ", "_"))
    //Array("a", "b", "c") ==> "a", "b", "c"

    val dfWithRightColumnNames = df.toDF(columnsWithtoutSpaces:_*)
    //dfWithRightColumnNames.show()
    //dfWithRightColumnNames.write.partitionBy("Sales_Channel").mode(SaveMode.Overwrite).parquet("result")
    val parquetDF = sparkSession.read.parquet("result")
    //parquetDF.printSchema()
    //parquetDF.show
    //df.show()
}
}
