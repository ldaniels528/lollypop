package com.lollypop.runtime.instructions.conditions

import com.lollypop.die
import com.lollypop.language.models.Inequality

import java.util.{Date, UUID}
import scala.annotation.tailrec

/**
 * Represents a run-time Inequality
 */
trait RuntimeInequality extends Inequality with RuntimeCondition

/**
 * Run-time Inequality Companion
 */
object RuntimeInequality {

  /**
   * Option Enrichment
   * @param optionA the host field or value
   */
  final implicit class OptionComparator(val optionA: Option[Any]) extends AnyVal {
    // TODO convert to Any >= Any

    def >[A <: Comparable[A]](optionB: Option[Any]): Boolean = boilerplate(optionB) {
      case (a: Date, b: Date) => a.compareTo(b) > 0
      case (a: Date, b: Number) => a.getTime > b.doubleValue()
      case (a: Number, b: Date) => a.doubleValue() > b.getTime
      case (a: Number, b: Number) => a.doubleValue() > b.doubleValue()
      case (a: String, b: String) => a.compareTo(b) > 0
      case (a: UUID, b: UUID) => a.compareTo(b) > 0
      case (a, b) => fail(a, b, operator = ">")
    }

    def >=[A <: Comparable[A]](optionB: Option[Any]): Boolean = boilerplate(optionB) {
      case (a: Date, b: Date) => a.compareTo(b) >= 0
      case (a: Date, b: Number) => a.getTime >= b.doubleValue()
      case (a: Number, b: Date) => a.doubleValue() >= b.getTime
      case (a: Number, b: Number) => a.doubleValue() >= b.doubleValue()
      case (a: String, b: String) => a.compareTo(b) >= 0
      case (a: UUID, b: UUID) => a.compareTo(b) >= 0
      case (a, b) => fail(a, b, operator = ">=")
    }

    def <(optionB: Option[Any]): Boolean = boilerplate(optionB) {
      case (a: Char, b: Char) => a.compareTo(b) < 0
      case (a: Date, b: Date) => a.compareTo(b) < 0
      case (a: Date, b: Number) => a.getTime < b.doubleValue()
      case (a: Number, b: Date) => a.doubleValue() < b.getTime
      case (a: Number, b: Number) => a.doubleValue() < b.doubleValue()
      case (a: String, b: String) => a.compareTo(b) < 0
      case (a: UUID, b: UUID) => a.compareTo(b) < 0
      case (a, b) => fail(a, b, operator = "<")
    }

    def <=[A <: Comparable[A]](optionB: Option[Any]): Boolean = boilerplate(optionB) {
      case (a: Date, b: Date) => a.compareTo(b) <= 0
      case (a: Date, b: Number) => a.getTime <= b.doubleValue()
      case (a: Number, b: Date) => a.doubleValue() <= b.getTime
      case (a: Number, b: Number) => a.doubleValue() <= b.doubleValue()
      case (a: String, b: String) => a.compareTo(b) <= 0
      case (a: UUID, b: UUID) => a.compareTo(b) <= 0
      case (a, b) => fail(a, b, operator = "<=")
    }

    @tailrec
    def unwrapMe: Option[Any] = optionA match {
      case Some(o: Option[_]) => o.unwrapMe
      case x => x
    }

    private def boilerplate(optionB: Option[Any])(f: PartialFunction[(Any, Any), Boolean]): Boolean = {
      (for {
        a <- optionA.unwrapMe
        b <- optionB.unwrapMe
      } yield f(a, b)).contains(true)
    }

    private def fail(a: Any, b: Any, operator: String): Nothing = {
      die(s"Could not compare: '$a' $operator '$b' (${Option(a).map(_.getClass.getName).orNull}, ${Option(b).map(_.getClass.getName).orNull})")
    }

  }

}
