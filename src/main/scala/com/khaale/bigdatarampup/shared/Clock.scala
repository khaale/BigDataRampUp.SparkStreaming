package com.khaale.bigdatarampup.shared

import java.util.Date

/**
  * Created by Aleksander_Khanteev on 5/25/2016.
  */
object Clock {

  var clockImpl: ClockImpl = new RealClockImpl()

  def getTime: Long  = clockImpl.getTime
}

abstract class ClockImpl {
  def getTime: Long
}

class RealClockImpl extends ClockImpl {
  def getTime: Long = new Date().getTime
}
