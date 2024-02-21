package com.lollypop.runtime

import com.lollypop.language.dieIllegalType
import com.lollypop.runtime.conversions.ScalaConversion

import java.util.Timer
import scala.concurrent.Future

/**
 * Represents a task that is continuously running or will be completed at some point in the future
 * @example {{{
 *  val timerB =
 *     after Duration('1 seconds')
 *         every Duration('3 seconds')
 *             changeScreenColor(Color.GREEN)
 * }}}
 */
trait Eventual extends ScalaConversion {

  def cancel(): Unit

  def isCancellable: Boolean = false

  def isCompleted: Boolean

  def isRepeatable: Boolean = false

  def task: Any

  def toScala: Any = task

}

/**
 * Eventual Companion
 */
object Eventual {

  def apply(task: Any): Eventual = task match {
    case future: Future[_] => FutureEventual(future)
    case runnable: Runnable =>
      val thread = new Thread(runnable)
      thread.setDaemon(true)
      thread.start()
      ThreadEventual(thread)
    case thread: Thread => ThreadEventual(thread)
    case timer: Timer => TimerEventual(timer)
    case x => dieIllegalType(x)
  }

  def unapply(evt: Eventual): Option[Any] = Option(evt.task)

}

/**
 * Future-based Eventual
 * @param task the provided [[Future]]
 */
case class FutureEventual[T](task: Future[T]) extends Eventual {
  override def cancel(): Unit = ()

  override def isCompleted: Boolean = task.isCompleted
}

/**
 * Timer-based Eventual
 * @param task the provided [[Timer]]
 */
case class TimerEventual(task: Timer) extends Eventual {
  override def cancel(): Unit = task.cancel()

  override def isCancellable = true

  override def isCompleted = false

  override def isRepeatable = true
}

/**
 * Thread-based Eventual
 * @param task the provided [[Thread]]
 */
case class ThreadEventual(task: Thread) extends Eventual {
  override def cancel(): Unit = task.interrupt()

  override def isCancellable = true

  override def isCompleted: Boolean = !task.isAlive || !task.isInterrupted
}
