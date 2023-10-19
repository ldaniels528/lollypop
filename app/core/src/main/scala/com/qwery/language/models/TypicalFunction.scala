package com.qwery.language.models

import com.qwery.runtime.instructions.functions.{AnonymousFunction, NamedFunction}

/**
 * Represents a typical function
 */
trait TypicalFunction extends Function {

  /**
   * @return the collection of function [[ParameterLike parameters]]
   */
  def params: Seq[ParameterLike]

  /**
   * @return the function [[Instruction code]]
   */
  def code: Instruction

}

object TypicalFunction {

  /**
   * Creates a new anonymous function
   * @param params the collection of function [[ParameterLike parameters]]
   * @param code   the function [[Instruction code]]
   * @return a new anonymous function
   */
  def apply(params: Seq[ParameterLike], code: Instruction): TypicalFunction = {
    AnonymousFunction(params, code)
  }

  /**
   * Creates a new named function
   * @param name   the name of the function
   * @param params the collection of function [[ParameterLike parameters]]
   * @param code   the function [[Instruction code]]
   * @return a new anonymous function
   */
  def apply(name: String, params: Seq[ParameterLike], code: Instruction): TypicalFunction = {
    NamedFunction(name, params, code, returnType_? = None)
  }

}