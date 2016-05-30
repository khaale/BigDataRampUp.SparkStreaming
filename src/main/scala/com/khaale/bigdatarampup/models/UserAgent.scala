package com.khaale.bigdatarampup.models

/**
  * Created by Aleksander_Khanteev on 5/24/2016.
  */

case class UserAgent(uaType:String, uaFamily:String, osName:String, device:String)

object UserAgent {

  def apply(rawUserAgent:String): UserAgent = {
    val ua = eu.bitwalker.useragentutils.UserAgent.parseUserAgentString(rawUserAgent)

    val uaType = if (ua.getBrowser != null) ua.getBrowser.getBrowserType.getName else "Unknown"
    val uaFamily = if (ua.getBrowser != null) ua.getBrowser.getGroup.getName else "Unknown"
    val osName = if (ua.getOperatingSystem != null) ua.getOperatingSystem.getName else "Unknown"
    val device = if (ua.getOperatingSystem != null) ua.getOperatingSystem.getDeviceType.getName else "Unknown"

    UserAgent(uaType, uaFamily, osName, device)
  }

  val empty = new UserAgent("unknown", "Unknown", "Unknown", "Unknown")
}
