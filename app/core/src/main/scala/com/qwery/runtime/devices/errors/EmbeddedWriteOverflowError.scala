package com.qwery.runtime.devices.errors

import com.qwery.QweryException

class EmbeddedWriteOverflowError(required: Long, allocated: Long)
  extends QweryException(s"Embedded write overflow: $required > $allocated")