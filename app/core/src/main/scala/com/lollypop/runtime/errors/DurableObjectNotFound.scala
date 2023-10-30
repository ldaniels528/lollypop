package com.lollypop.runtime.errors

import com.lollypop.LollypopException
import com.lollypop.runtime.DatabaseObjectRef

class DurableObjectNotFound(ref: DatabaseObjectRef)
  extends LollypopException(s"Durable object '${ref.toSQL}' does not exist")