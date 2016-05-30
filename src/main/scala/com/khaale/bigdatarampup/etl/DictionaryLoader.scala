package com.khaale.bigdatarampup.etl

import com.khaale.bigdatarampup.models._
import com.khaale.bigdatarampup.shared.DictionarySettings
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, SQLContext}

/**
  * Created by Aleksander_Khanteev on 5/21/2016.
  */
class DictionaryLoader(sc:SparkContext, dictSettings: DictionarySettings) extends Serializable {

  val sqlc = SQLContext.getOrCreate(sc)
  import sqlc.implicits._

  def loadTags(): Dataset[DicTags] = {
    sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", "\t")
      .load(dictSettings.tagsPath)
      .select(
        $"ID".alias("id"),
        $"Keyword Value".alias("keywords"),
        $"Keyword Status".alias("status"),
        $"Pricing Type".alias("pricingType"),
        $"Keyword Match Type".alias("matchType"),
        $"Destination URL".alias("destinationUrl")
      ).map(r => DicTags(r.getLong(0), r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getString(5)))
      .toDS()
  }

  def loadCities(): Dataset[DicCity] = {
    sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", "\t")
      .load(dictSettings.citiesPath)
      .select(
        $"Id".alias("id"),
        $"City".alias("name"),
        $"State Id".alias("stateId"),
        $"Population".alias("population"),
        $"Area".alias("area").cast(DataTypes.FloatType),
        $"Density".alias("density").cast(DataTypes.FloatType),
        $"Latitude".alias("latitude").cast(DataTypes.FloatType),
        $"Longitude".alias("longitude").cast(DataTypes.FloatType)
      )
      .as[DicCity]
  }

  def loadStates(): Dataset[DicState] = {
    sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", "\t")
      .load(dictSettings.statesPath)
      .select(
        $"Id".alias("id"),
        $"State".alias("name"),
        $"Population".alias("population"),
        $"GSP".alias("gsp"))
      .as[DicState]
  }

  def loadLogTypes(): Dataset[DicLogType] = {
    sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", "\t")
      .load(dictSettings.logTypesPath)
      .select(
        $"Stream Id".alias("id"),
        $"Type".alias("name"))
      .as[DicLogType]
  }

  def loadAdExchanges(): Dataset[DicAdExchange] = {
    sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", "\t")
      .load(dictSettings.adExchangesPath)
      .select(
        $"Id".alias("id"),
        $"Name".alias("name"),
        $"Description".alias("description"))
      .as[DicAdExchange]
  }
}


