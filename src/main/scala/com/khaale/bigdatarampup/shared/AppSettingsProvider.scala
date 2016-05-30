package com.khaale.bigdatarampup.shared

import java.io.File

import com.khaale.bigdatarampup.etl.KafkaStreamerSettings
import com.khaale.bigdatarampup.shared.cassandra.CassandraSettings
import com.typesafe.config.ConfigFactory

/**
  * Created by Aleksander_Khanteev on 5/23/2016.
  */
class AppSettingsProvider(configPath:String) {

  private val cfgFile = new File(configPath)

  private val appCfg = cfgFile match  {
    case x if x.exists() => ConfigFactory.parseFile(cfgFile).getConfig("app")
    case _ => throw new IllegalArgumentException(s"Configuration file '$configPath' does not exist")
  }

  def dictionarySettings: DictionarySettings = {
    if (!appCfg.hasPath("dict")) {
      return DictionarySettings("hdfs://sandbox.hortonworks.com/data/advertising/dic")
    }

    val dictCfg = appCfg.getConfig("dict")
    DictionarySettings(dictCfg.getString("directory"))
  }

  def getFacebookSettings: Option[FacebookSettings] = {

    if (!appCfg.hasPath("facebook")) {
      return None
    }

    val fbCfg = appCfg.getConfig("facebook")
    Some(FacebookSettings(fbCfg.getString("token")))
  }

  def isTestRun: Boolean = {
    appCfg.hasPath("is-test-run") && appCfg.getBoolean("is-test-run")
  }

  def getInputPath: String = {
    if (appCfg.hasPath("input-path")) appCfg.getString("input-path") else "/data/advertising/city_date_tagIds"
  }

  def getOutputPath: String = {
    if (appCfg.hasPath("output-path")) appCfg.getString("output-path") else "/data/advertising/city_date_event_attendance"
  }

  def getKafkaSettings:Option[KafkaStreamerSettings] = {

    if (!appCfg.hasPath("kafka")) {
      return None
    }

    val cfg = appCfg.getConfig("kafka")
    Some(KafkaStreamerSettings(cfg.getString("broker"), cfg.getString("topic")))
  }

  def cassandraSettings: Option[CassandraSettings] = {
    if (!appCfg.hasPath("cassandra")) {
      return None
    }

    val cfg = appCfg.getConfig("cassandra")
    Some(CassandraSettings(cfg.getString("host"), cfg.getString("keyspace")))
  }
}

case class FacebookSettings(token:String)

case class DictionarySettings(
                               statesPath:String,
                               citiesPath:String,
                               logTypesPath:String,
                               adExchangesPath:String,
                               tagsPath:String
                               ) {
}
object DictionarySettings {
  def apply(dictionaryDirPath: String) = new DictionarySettings(
    dictionaryDirPath + "/states.us.txt",
    dictionaryDirPath + "/city.us.txt",
    dictionaryDirPath + "/log.type.txt",
    dictionaryDirPath + "/ad.exchange.txt",
    dictionaryDirPath + "/user.profile.tags.us.txt"
    )
}
