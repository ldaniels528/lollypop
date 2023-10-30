package com.lollypop.runtime.errors

import com.lollypop.LollypopException

class MatrixDimensionMismatchError()
  extends LollypopException("Matrices must have the same number of rows and columns")