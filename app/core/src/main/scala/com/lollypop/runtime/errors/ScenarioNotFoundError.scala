package com.lollypop.runtime.errors

import com.lollypop.LollypopException
import com.lollypop.language.models.Expression

class ScenarioNotFoundError(title: String, titleExpr: Expression)
  extends LollypopException(s"Referenced scenario `$title` was not found ${titleExpr.toMessage}".trim)