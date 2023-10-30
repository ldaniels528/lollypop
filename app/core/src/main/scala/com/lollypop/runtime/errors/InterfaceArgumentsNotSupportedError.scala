package com.lollypop.runtime.errors

import com.lollypop.LollypopException
import com.lollypop.language.models.Instruction

class InterfaceArgumentsNotSupportedError(instruction: Instruction)
  extends LollypopException(s"Interface instances do not support arguments ${instruction.toMessage}".trim)