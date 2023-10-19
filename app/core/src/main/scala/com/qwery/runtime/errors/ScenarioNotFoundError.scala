package com.qwery.runtime.errors

import com.qwery.QweryException
import com.qwery.language.models.Expression

class ScenarioNotFoundError(title: String, titleExpr: Expression)
  extends QweryException(s"Referenced scenario `$title` was not found ${titleExpr.toMessage}".trim)