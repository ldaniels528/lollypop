package com.lollypop.runtime.errors

import com.lollypop.LollypopException

class DivisionByZeroError(reason: String)
  extends LollypopException(s"Division by zero: $reason")