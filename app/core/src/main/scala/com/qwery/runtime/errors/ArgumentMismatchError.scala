package com.qwery.runtime.errors

import com.qwery.QweryException

class ArgumentMismatchError(message: String)
  extends QweryException(message)