package com.qwery.runtime.devices.errors

import com.qwery.QweryException
import com.qwery.util.StringRenderHelper.StringRenderer

class UnsupportedFormatError(value: Any) extends QweryException(s"Unsupported format '${value.render}'")