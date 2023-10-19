package com.qwery.runtime.errors

import com.qwery.QweryException

class MatrixDimensionMismatchError()
  extends QweryException("Matrices must have the same number of rows and columns")