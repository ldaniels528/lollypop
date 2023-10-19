package com.qwery.runtime.devices.errors

import com.qwery.QweryException

class UniqueKeyCannotBeDisabledError(columnName: String)
  extends QweryException(s"The unique column '$columnName' cannot be disabled")
