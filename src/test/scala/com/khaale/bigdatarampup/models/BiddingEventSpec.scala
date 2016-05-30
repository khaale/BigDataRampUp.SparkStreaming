package com.khaale.bigdatarampup.models

import org.apache.spark.sql.types.Decimal
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Aleksander_Khanteev on 5/24/2016.
  */
class BiddingEventSpec extends FlatSpec with Matchers  {

  val message =
    "11baa543d120063f0f161b54232c7202\t" +
      "20130611232904865\t" +
      "Vh27Z5sxDva4Jg2\t" +
      "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)\t" +
      "118.254.16.*\t" +
      "201\t" +
      "209\t" +
      "3\t" +
      "3KFal19xGq1m1YdI5SqfNX\t" +
      "1df08a1077dbcc6b147b6b2a0889f999\t" +
      "null\t" +
      "Digital_F_Width1\t" +
      "1000\t" +
      "90\t" +
      "0\t" +
      "0\t" +
      "31\t" +
      "c46090c887c257b61ab1fa11baee91d8\t" +
      "241\t" +
      "3427\t" +
      "282825712806\t" +
      "0"

  "A correct parsed click stream message" should "have base fields filled" in {
    //act
    val result = BiddingEvent(message)
    //assert
    result.bidId should equal("11baa543d120063f0f161b54232c7202")
    result.ts should equal(1370993344865L)
    result.ipinyouId should equal("Vh27Z5sxDva4Jg2")
    result.userAgent should equal("Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)")
    result.region should equal(201)
    result.city should equal(209)
    result.adExchange should equal(3)
    result.payingPrice should equal(Decimal(31))
    result.biddingPrice should equal(Decimal(241))
    result.streamId should equal(0)
  }

}
