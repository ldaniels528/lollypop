package com.lollypop.runtime

import org.scalatest.funspec.AnyFunSpec

import java.util.{Timer, TimerTask}
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/**
 * Eventual Test Suite
 */
class EventualTest extends AnyFunSpec {

  describe(classOf[Eventual].getSimpleName) {

    it("should wrap Future[T] instances") {
      val promise = Future.successful(1)
      val eventual = Eventual(promise)

      assert(!eventual.isCancellable)
      assert(eventual.isCompleted)
      assert(!eventual.isRepeatable)
      assert(eventual.toScala == promise)
    }

    it("should wrap Runnable instances") {
      var counter: Int = 0
      val eventual = Eventual(new Runnable {
        override def run(): Unit = counter += 1
      })
      giveItSomeTime()

      info(s"counter = $counter")

      assert(counter == 1)
      assert(eventual.isCancellable)
      assert(eventual.isCompleted)
      assert(!eventual.isRepeatable)
    }

    it("should wrap Thread instances") {
      var counter: Int = 0
      val thread = new Thread(() => counter += 1)
      val eventual = Eventual(thread)
      giveItSomeTime()

      info(s"counter = $counter")

      assert(counter == 1)
      assert(eventual.isCancellable)
      assert(eventual.isCompleted)
      assert(!eventual.isRepeatable)
    }

    it("should wrap one-time TimerTask instances") {
      var counter: Int = 0
      val timer = new Timer()
      timer.schedule(new TimerTask {
        override def run(): Unit = counter += 1
      }, 0.second.toMillis)
      val eventual = Eventual(timer)
      giveItSomeTime()

      info(s"counter = $counter")

      assert(counter == 1)
      assert(eventual.isCancellable)
      assert(!eventual.isCompleted)
      assert(eventual.isRepeatable)
    }

    it("should wrap repeated TimerTask instances") {
      var counter: Int = 0
      val timer = new Timer()
      timer.schedule(new TimerTask {
        override def run(): Unit = counter += 1
      }, 0.second.toMillis, 50.millis.toMillis)
      val eventual = Eventual(timer)
      giveItSomeTime()

      info(s"counter = $counter")

      try {
        assert(counter > 0)
        assert(eventual.isCancellable)
        assert(!eventual.isCompleted)
        assert(eventual.isRepeatable)
      } finally eventual.cancel()
    }

  }

  private def giveItSomeTime(): Unit = Thread.sleep(100)

}