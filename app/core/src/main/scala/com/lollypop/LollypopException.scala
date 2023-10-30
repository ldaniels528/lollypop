package com.lollypop

/**
 * Base class for all Lollypop exceptions
 * @param message the error message
 * @param cause   the error cause
 */
class LollypopException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

object LollypopException {
  def apply(message: String, cause: Throwable = null): LollypopException = new LollypopException(message, cause)
}
