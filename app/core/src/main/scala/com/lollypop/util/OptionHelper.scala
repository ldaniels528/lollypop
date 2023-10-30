package com.lollypop.util

import scala.language.implicitConversions

/**
 * Option Helper
 * @author lawrence.daniels@gmail.com
 */
object OptionHelper {

  object implicits {

    object risky {

      final implicit def value2Option[T](value: T): Option[T] = Option(value)

    }
  }

  /**
   * Option Enrichment
   * @param optionA the given [[Option option]]
   */
  final implicit class OptionEnrichment[A](val optionA: Option[A]) extends AnyVal {

    @inline def ??[B <: A](optionB: => Option[B]): Option[A] = if (optionA.nonEmpty) optionA else optionB

    @inline def ||(value: => A): A = optionA getOrElse value

  }

}