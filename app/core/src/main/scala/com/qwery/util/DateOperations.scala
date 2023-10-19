package com.qwery.util

import java.util.Date
import scala.concurrent.duration.{DurationLong, FiniteDuration}

/**
 * Date Operations
 */
object DateOperations {

  /**
   * Date Math Utilities
   * @param host the host [[Date date]]
   */
  final implicit class DateMathematics(val host: Date) extends AnyVal {

    @inline def -(date: Date): FiniteDuration = (host.getTime - date.getTime).millis

    @inline def +(interval: FiniteDuration): Date = DateHelper.from(host.getTime + interval.toMillis)

    @inline def -(interval: FiniteDuration): Date = DateHelper.from(host.getTime - interval.toMillis)

  }

}
