package com.khaale.bigdatarampup.etl

import com.khaale.bigdatarampup.models._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by Aleksander_Khanteev on 5/24/2016.
  */
object BiddingEventEnrichmentProcessor {

  def process(sc:SparkContext, dictionaryLoader: DictionaryLoader)(input:DStream[BiddingEvent]) = {


    val dictProvider = new DictionaryProvider(sc, dictionaryLoader)
    val states = dictProvider.getStates
    val cities = dictProvider.getCities
    val logTypes = dictProvider.getLogTypes
    val adExchanges = dictProvider.getAdExchanges
    val tags = dictProvider.getTags

    input.transform(
      rdd => {

        /*
        val dictProvider = new DictionaryProvider(sc, dictionaryLoader)
        val states = dictProvider.getStates
        val cities = dictProvider.getCities
        val logTypes = dictProvider.getLogTypes
        val adExchanges = dictProvider.getAdExchanges
        val tags = dictProvider.getTags
        */

        rdd.map(x => {
          val city = cities.value.getOrElse(x.city, DicCity.empty)
          val state = states.value.getOrElse(city.stateId, DicState.empty)
          BiddingBundle(
            x,
            state,
            city,
            logTypes.value.getOrElse(x.streamId, DicLogType.empty),
            adExchanges.value.getOrElse(x.adExchange, DicAdExchange.empty),
            tags.value.getOrElse(x.userTags, DicTags.empty),
            UserAgent(x.userAgent))
        })
      }
    )
  }
}

class DictionaryProvider(sparkContext: SparkContext, dictionaryLoader: DictionaryLoader) extends Serializable {

  @volatile private var states: Broadcast[Map[Int,DicState]] = null
  @volatile private var cities: Broadcast[Map[Int,DicCity]] = null
  @volatile private var tags: Broadcast[Map[Long,DicTags]] = null
  @volatile private var adExchanges: Broadcast[Map[Byte,DicAdExchange]] = null
  @volatile private var logTypes: Broadcast[Map[Int,DicLogType]] = null

  private def getInstance[A](predicate:() => Boolean)(createFunc: => Unit)(instanceFunc: => Broadcast[A]): Broadcast[A] = {
    if (predicate()) {
      synchronized {
        if (predicate()) {
          createFunc
        }
      }
    }
    instanceFunc
  }

  private def broadcastAsMap[B,A](data:Dataset[A])(getId:A=>B): Broadcast[Map[B,A]] = {

    val map = data.collect().map(x => getId(x) -> x).toMap
    sparkContext.broadcast(map)
  }

  def getStates = getInstance (() => states == null ) {states = broadcastAsMap(dictionaryLoader.loadStates())(x => x.id) } { states }
  def getCities = getInstance (() => cities == null ) {cities = broadcastAsMap(dictionaryLoader.loadCities())(x => x.id) } { cities }
  def getLogTypes = getInstance (() => logTypes == null ) {logTypes = broadcastAsMap(dictionaryLoader.loadLogTypes())(x => x.id) } { logTypes }
  def getAdExchanges = getInstance (() => adExchanges == null ) {adExchanges = broadcastAsMap(dictionaryLoader.loadAdExchanges())(x => x.id) } { adExchanges }
  def getTags = getInstance (() => tags == null ) {tags = broadcastAsMap(dictionaryLoader.loadTags())(x => x.id) } { tags }
}

