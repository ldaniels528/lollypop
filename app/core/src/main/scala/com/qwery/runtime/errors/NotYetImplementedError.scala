package com.qwery.runtime.errors

import com.qwery.QweryException

class NotYetImplementedError() extends QweryException(message = "an implementation is missing")