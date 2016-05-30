package com.khaale.bigdatarampup.testing.fixtures

import com.khaale.bigdatarampup.models.{DicAdExchange, DicTags, UserAgent, _}
import org.apache.spark.sql.types.Decimal

/**
  * Created by Aleksander_Khanteev on 5/25/2016.
  */
object BiddingFixture {

  def createEvent(ipinyouId:String = "VhkR1aq4DoCIQOE", ts:Long = 1370721600000L) = new BiddingEvent(
    "123",
    1370721600000L,
    "VhkR1aq4DoCIQOE",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "182.18.2.*",
    1,
    1,
    3,
    "31xSTvprdN1RFt",
    "4bca76c166cc4c5091839c5c4c5e8e6b",
    "null",
    "Ent_F_Upright",
    300,
    250,
    0,
    0,
    Decimal(50),
    "44966cc8da1ed40c95d59e863c8c75f0",
    Decimal(300),
    3386,
    282162995497L,
    0)

  def createBundle(ipinyouId:String = "VhkR1aq4DoCIQOE", ts:Long = 1370721600000L) = BiddingBundle(
    createEvent(ipinyouId,ts),
    DicState.empty,
    DicCity.empty,
    DicLogType.empty,
    DicAdExchange.empty,
    DicTags.empty,
    UserAgent.empty
  )

  def createSession(ipinyouId:String = "VhkR1aq4DoCIQOE", ts:Long = 1370721600000L, state:Byte = BiddingSession.stateNew): BiddingSession = {

    val session = BiddingSession(Seq(createBundle(ipinyouId, ts)))
    state match {
      case BiddingSession.stateNew => session
      case BiddingSession.stateOpened => session.open
      case BiddingSession.stateClosed => session.close()
    }
  }

}
