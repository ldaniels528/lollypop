package com.qwery.runtime.instructions.conditions

import com.qwery.language.models.Expression
import com.qwery.runtime.Scope

/**
 * Represents a unit of verification (e.g. unit test)
 */
trait Verification extends RuntimeCondition {

  def condition: Expression

  def determineMismatches(scope: Scope): List[String]

  def title: Option[Expression]

}
