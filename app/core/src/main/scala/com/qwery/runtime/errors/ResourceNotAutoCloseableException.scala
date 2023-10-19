package com.qwery.runtime.errors

import com.qwery.QweryException
import com.qwery.util.StringRenderHelper.StringRenderer

class ResourceNotAutoCloseableException(val res: Any)
  extends QweryException(s"Resource '${res.render}' does not inherit '${classOf[AutoCloseable].getName}'")
