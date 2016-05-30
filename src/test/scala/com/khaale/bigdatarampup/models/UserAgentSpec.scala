package com.khaale.bigdatarampup.models

import org.scalatest.{FlatSpec, FunSuite, Matchers}

/**
  * Created by Aleksander_Khanteev on 5/24/2016.
  */
class UserAgentSpec extends FlatSpec with Matchers  {

  "A correct user agent string" should "be parsed" in {
    //arrange
    val userAgentString = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
    //act
    val result = UserAgent(userAgentString)
    //assert
    result.uaType should equal("Browser")
    result.uaFamily should equal("Chrome")
    result.osName should equal("Windows 7")
    result.device should equal("Computer")
  }

  "An incorrect user agent string" should "be parsed as unknown" in {
    //arrange
    val userAgentString = ""
    //act
    val result = UserAgent(userAgentString)
    //assert
    result.uaType should equal("unknown")
    result.uaFamily should equal("Unknown")
    result.osName should equal("Unknown")
    result.device should equal("Unknown")
  }

}
