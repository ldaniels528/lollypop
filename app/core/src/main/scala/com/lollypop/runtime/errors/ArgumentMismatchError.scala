package com.lollypop.runtime.errors

import com.lollypop.LollypopException

class ArgumentMismatchError(message: String)
  extends LollypopException(message)