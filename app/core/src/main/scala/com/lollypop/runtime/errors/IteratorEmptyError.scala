package com.lollypop.runtime.errors

import com.lollypop.LollypopException

class IteratorEmptyError()
  extends LollypopException("Iterator is empty")