package com.lollypop.runtime.errors

import com.lollypop.LollypopException
import com.lollypop.util.StringRenderHelper.StringRenderer

class ResourceNotAutoCloseableException(val res: Any)
  extends LollypopException(s"Resource '${res.render}' does not inherit '${classOf[AutoCloseable].getName}'")
