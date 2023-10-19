package com.qwery.runtime.errors

import com.qwery.QweryException

class DivisionByZeroError(reason: String)
  extends QweryException(s"Division by zero: $reason")