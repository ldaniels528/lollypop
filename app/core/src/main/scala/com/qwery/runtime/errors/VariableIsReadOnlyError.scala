package com.qwery.runtime.errors

import com.qwery.QweryException

class VariableIsReadOnlyError(name: String)
  extends QweryException(s"Variable '$name' is read-only")