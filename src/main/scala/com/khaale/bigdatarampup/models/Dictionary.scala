package com.khaale.bigdatarampup.models

/**
  * Created by Aleksander_Khanteev on 5/21/2016.
  */
case class DicTags(id: Long, keywords:Seq[String], status:String, pricingType:String, matchType:String, destinationUrl:String)
object DicTags {
  val empty = DicTags(0,Seq.empty[String],"","","","")

  def apply(id: Long, keywords:String, status:String, pricingType:String, matchType:String, destinationUrl:String) = {
    new DicTags(id, keywords.split(","), status, pricingType, matchType, destinationUrl)
  }
}

case class DicCity(id: Int, name: String, stateId: Int, population:Int, area:Float, density:Float, latitude:Float, longitude:Float) {
  def getSearchDistance : Int = {
    Math.sqrt(area).toInt * 1000
  }
}
object DicCity {
  val empty: DicCity = {
    DicCity(0, "", 0,0,0,0,0,0)
  }
}


case class DicAdExchange(id:Byte, name:String, description:String)
object DicAdExchange {
  val empty = DicAdExchange(0,"","")
}

case class DicLogType(id:Int, name:String)
object DicLogType {
  val empty = DicLogType(0,"")
}

case class DicState(id:Int, name:String, population:Int, gsp:Int)
object DicState {
  val empty = DicState(0,"unknown",0,0)
}
