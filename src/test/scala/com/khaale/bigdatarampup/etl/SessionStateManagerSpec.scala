package com.khaale.bigdatarampup.etl

import java.util.Date

import com.khaale.bigdatarampup.models.BiddingSession
import com.khaale.bigdatarampup.shared.Clock
import com.khaale.bigdatarampup.testing.fixtures.BiddingFixture
import com.khaale.bigdatarampup.testing.mocks.ManualClockImpl
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * Created by Aleksander_Khanteev on 5/24/2016.
  */
class SessionStateManagerSpec extends FlatSpec with Matchers with BeforeAndAfter  {

  var clock: ManualClockImpl = new ManualClockImpl()

  before {
    //set up and reset clock, which is used inside of SessionStateManager
    Clock.clockImpl = clock
    clock.time = new Date().getTime
  }

  "A bundle" should "produce a new session" in {
    //arrange
    val bundle = BiddingFixture.createBundle()
    //act
    val result = SessionStateManager.updateFunc(Seq(bundle), None)
    //assert
    result.isDefined should equal(true)
    result.get.status should equal(BiddingSession.stateNew)
    result.get.tsStart should equal(bundle.ts)
    result.get.tsEnd should equal(bundle.ts)
  }

  "A bundle" should "open existing new session" in {
    //arrange
    val bundle = BiddingFixture.createBundle()
    val session = BiddingFixture.createSession()
    clock.advance(1000)
    //act
    val result = SessionStateManager.updateFunc(Seq(bundle), Some(session))
    //assert
    result.isDefined should equal(true)
    result.get.status should equal(BiddingSession.stateOpened)
    result.get.tsCurrent should be > session.tsCurrent
  }

  "A new session" should "be only opened when session timeout is not expired" in {
    //arrange
    val session = BiddingFixture.createSession()
    clock.advance(1000)
    //act
    val result = SessionStateManager.updateFunc(Seq.empty, Some(session))
    //assert
    result.isDefined should equal(true)
    result.get.status should equal(BiddingSession.stateOpened)
    result.get.tsCurrent should equal(session.tsCurrent)
  }

  "A not closed session" should "be closed when timeout is expired" in {
    //arrange
    val session = BiddingFixture.createSession()
    clock.advance(BiddingSession.sessionTimeout + 1000)
    //act
    val result = SessionStateManager.updateFunc(Seq.empty, Some(session))
    //assert
    result.isDefined should equal(true)
    result.get.status should equal(BiddingSession.stateClosed)
  }

  "A closed session" should "be removed" in {
    //arrange
    val session = BiddingFixture.createSession(state = BiddingSession.stateClosed)
    clock.advance(BiddingSession.sessionTimeout + 1000)
    //act
    val result = SessionStateManager.updateFunc(Seq.empty, Some(session))
    //assert
    result.isEmpty should equal(true)
  }
}
