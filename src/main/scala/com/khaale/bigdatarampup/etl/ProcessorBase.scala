package com.khaale.bigdatarampup.etl
import org.apache.spark.{Accumulator, SparkContext}

/**
  * Created by Aleksander_Khanteev on 5/24/2016.
  */
trait Countable {

  val counters:scala.collection.mutable.Map[String,Accumulator[Long]] = scala.collection.mutable.HashMap.empty

  protected def getOrCreateCounter(sc:SparkContext, name:String):Accumulator[Long] = {

    val key = this.getClass.getSimpleName + " -> " + name
    counters.get(key) match {
      case Some(x) => x
      case None =>
        val acc = sc.accumulator(0L, key)
        counters.put(key, acc)
        acc
    }
  }
}












