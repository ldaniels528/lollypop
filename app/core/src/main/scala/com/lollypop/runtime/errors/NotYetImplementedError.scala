package com.lollypop.runtime.errors

import com.lollypop.LollypopException

class NotYetImplementedError() extends LollypopException(message = "an implementation is missing")