package com.lollypop.runtime.devices.errors

import com.lollypop.LollypopException

class EmbeddedWriteOverflowError(required: Long, allocated: Long)
  extends LollypopException(s"Embedded write overflow: $required > $allocated")