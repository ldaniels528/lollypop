package com.lollypop.runtime.devices.errors

import com.lollypop.LollypopException

class UniqueKeyAlreadyExistsError(columnName: String, newValue: Option[Any])
  extends LollypopException(s"Unique constraint violation: Key '$columnName' ('${newValue.orNull}') already exists")