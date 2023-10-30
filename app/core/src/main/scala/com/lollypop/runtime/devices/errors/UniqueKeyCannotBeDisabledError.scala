package com.lollypop.runtime.devices.errors

import com.lollypop.LollypopException

class UniqueKeyCannotBeDisabledError(columnName: String)
  extends LollypopException(s"The unique column '$columnName' cannot be disabled")
