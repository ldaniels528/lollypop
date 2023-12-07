package com

/**
 * lollypop package object
 * @author lawrence.daniels@gmail.com
 */
package object lollypop {

  /**
   * Throws an exception
   * @param message the exception message
   * @param cause   the optional [[Throwable cause]] of the error
   * @return [[Nothing]]
   */
  def die[A](message: => String, cause: Throwable = null): A = throw LollypopException(message, cause)

}
