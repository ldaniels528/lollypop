package com.lollypop.runtime

import scala.language.implicitConversions

/**
 *  runtime implicits package object
 */
package object implicits {

  object risky {
    final implicit def value2Option[T](value: T): Option[T] = Option(value)
  }

}
