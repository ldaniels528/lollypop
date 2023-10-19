package com.qwery

/**
 * Base class for all Qwery exceptions
 * @param message the error message
 * @param cause   the error cause
 */
class QweryException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

object QweryException {
  def apply(message: String, cause: Throwable = null): QweryException = new QweryException(message, cause)
}
