package com.lollypop.runtime.devices.errors

import com.lollypop.LollypopException
import com.lollypop.util.StringRenderHelper.StringRenderer

class UnsupportedFormatError(value: Any) extends LollypopException(s"Unsupported format '${value.render}'")