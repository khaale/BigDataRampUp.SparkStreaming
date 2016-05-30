package com.khaale.bigdatarampup.testing.mocks

import java.util.Date

import com.khaale.bigdatarampup.shared.ClockImpl
import org.apache.spark.streaming.{ClockWrapper, StreamingContext}

/**
  * Created by Aleksander_Khanteev on 5/25/2016.
  */
class SparkClockImpl(ssc:StreamingContext) extends ClockImpl {

  def getTime:Long = ClockWrapper.getTime(ssc)
}

class ManualClockImpl(var time:Long = new Date().getTime) extends ClockImpl {

  def advance(millis:Long): Unit = {
    time = time + millis
  }

  def getTime:Long = time
}
