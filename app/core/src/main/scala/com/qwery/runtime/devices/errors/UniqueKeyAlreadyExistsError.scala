package com.qwery.runtime.devices.errors

import com.qwery.QweryException

class UniqueKeyAlreadyExistsError(columnName: String, newValue: Option[Any])
  extends QweryException(s"Unique constraint violation: Key '$columnName' ('${newValue.orNull}') already exists")