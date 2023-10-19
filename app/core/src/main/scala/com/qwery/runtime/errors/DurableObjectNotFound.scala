package com.qwery.runtime.errors

import com.qwery.QweryException
import com.qwery.runtime.DatabaseObjectRef

class DurableObjectNotFound(ref: DatabaseObjectRef)
  extends QweryException(s"Durable object '${ref.toSQL}' does not exist")