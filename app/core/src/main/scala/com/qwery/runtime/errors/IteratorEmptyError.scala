package com.qwery.runtime.errors

import com.qwery.QweryException

class IteratorEmptyError()
  extends QweryException("Iterator is empty")