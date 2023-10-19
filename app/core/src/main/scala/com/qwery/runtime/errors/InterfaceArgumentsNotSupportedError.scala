package com.qwery.runtime.errors

import com.qwery.QweryException
import com.qwery.language.models.Instruction

class InterfaceArgumentsNotSupportedError(instruction: Instruction)
  extends QweryException(s"Interface instances do not support arguments ${instruction.toMessage}".trim)