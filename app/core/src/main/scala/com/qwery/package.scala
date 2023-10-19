package com

/**
 * qwery package object
 * @author lawrence.daniels@gmail.com
 */
package object qwery {

  /**
   * Throws an exception
   * @param message the exception message
   * @param cause   the optional [[Throwable cause]] of the error
   * @return [[Nothing]]
   */
  def die[A](message: => String, cause: Throwable = null): A = throw QweryException(message, cause)

  object implicits {

    final implicit class MagicImplicits[A](val value: A) extends AnyVal {
      @inline def ~>[B](f: A => B): B = f(value)

    }

    final implicit class MagicBoolImplicits(val value: Boolean) extends AnyVal {

      @inline def ==>[A](result: A): Option[A] = if(value) Option(result) else None

    }

  }

}
