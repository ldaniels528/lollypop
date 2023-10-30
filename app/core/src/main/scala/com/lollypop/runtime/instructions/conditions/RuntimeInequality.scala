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

    @tailrec
    def unwrapMe: Option[Any] = optionA match {
      case Some(o: Option[_]) => o.unwrapMe
      case x => x
    }

    def >[A <: Comparable[A]](optionB: Option[Any]): Boolean = {
      (for {
        valueA <- optionA.unwrapMe
        valueB <- optionB.unwrapMe
      } yield {
        (valueA, valueB) match {
          case (a: Date, b: Date) => a.compareTo(b) > 0
          case (a: Date, b: Number) => a.getTime > b.doubleValue()
          case (a: Number, b: Date) => a.doubleValue() > b.getTime
          case (a: Number, b: Number) => a.doubleValue() > b.doubleValue()
          case (a: String, b: String) => a.compareTo(b) > 0
          case (a: UUID, b: UUID) => a.compareTo(b) > 0
          case (a, b) => die(s"Could not compare: '$a' > '$b' (${Option(a).map(_.getClass.getName).orNull}, ${Option(b).map(_.getClass.getName).orNull})")
        }
      }).contains(true)
    }

    def >=[A <: Comparable[A]](optionB: Option[Any]): Boolean = {
      (for {
        valueA <- optionA.unwrapMe
        valueB <- optionB.unwrapMe
      } yield {
        (valueA, valueB) match {
          case (a: Date, b: Date) => a.compareTo(b) >= 0
          case (a: Date, b: Number) => a.getTime >= b.doubleValue()
          case (a: Number, b: Date) => a.doubleValue() >= b.getTime
          case (a: Number, b: Number) => a.doubleValue() >= b.doubleValue()
          case (a: String, b: String) => a.compareTo(b) >= 0
          case (a: UUID, b: UUID) => a.compareTo(b) >= 0
          case (a, b) => die(s"Could not compare: '$a' >= '$b' (${Option(a).map(_.getClass.getName).orNull}, ${Option(b).map(_.getClass.getName).orNull})")
        }
      }).contains(true)
    }

    def <(optionB: Option[Any]): Boolean = {
      (for {
        valueA <- optionA.unwrapMe
        valueB <- optionB.unwrapMe
      } yield {
        (valueA, valueB) match {
          case (a: Char, b: Char) => a.compareTo(b) < 0
          case (a: Date, b: Date) => a.compareTo(b) < 0
          case (a: Date, b: Number) => a.getTime < b.doubleValue()
          case (a: Number, b: Date) => a.doubleValue() < b.getTime
          case (a: Number, b: Number) => a.doubleValue() < b.doubleValue()
          case (a: String, b: String) => a.compareTo(b) < 0
          case (a: UUID, b: UUID) => a.compareTo(b) < 0
          case (a, b) => die(s"Could not compare: '$a' < '$b' (${Option(a).map(_.getClass.getName).orNull}, ${Option(b).map(_.getClass.getName).orNull})")
        }
      }).contains(true)
    }

    def <=[A <: Comparable[A]](optionB: Option[Any]): Boolean = {
      (for {
        valueA <- optionA.unwrapMe
        valueB <- optionB.unwrapMe
      } yield {
        (valueA, valueB) match {
          case (a: Date, b: Date) => a.compareTo(b) <= 0
          case (a: Date, b: Number) => a.getTime <= b.doubleValue()
          case (a: Number, b: Date) => a.doubleValue() <= b.getTime
          case (a: Number, b: Number) => a.doubleValue() <= b.doubleValue()
          case (a: String, b: String) => a.compareTo(b) <= 0
          case (a: UUID, b: UUID) => a.compareTo(b) <= 0
          case (a, b) => die(s"Could not compare: '$a' <= '$b' (${Option(a).map(_.getClass.getName).orNull}, ${Option(b).map(_.getClass.getName).orNull})")
        }
      }).contains(true)
    }

  }

}
