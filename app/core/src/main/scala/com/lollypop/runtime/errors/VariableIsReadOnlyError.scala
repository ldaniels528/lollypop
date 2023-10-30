package com.lollypop.runtime.errors

import com.lollypop.LollypopException

class VariableIsReadOnlyError(name: String)
  extends LollypopException(s"Variable '$name' is read-only")