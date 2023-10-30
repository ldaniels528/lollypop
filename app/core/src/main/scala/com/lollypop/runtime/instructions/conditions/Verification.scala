package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.models.Expression
import com.lollypop.runtime.Scope

/**
 * Represents a unit of verification (e.g. unit test)
 */
trait Verification extends RuntimeCondition {

  def condition: Expression

  def determineMismatches(scope: Scope): List[String]

  def title: Option[Expression]

}
