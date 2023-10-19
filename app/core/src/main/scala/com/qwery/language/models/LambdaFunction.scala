package com.qwery.language.models

/**
 * Represents a lambda function
 * @example {{{
 *   (n: Int) => n + 1
 * }}}
 * @example {{{
 *   { (n: Int) => n + 1 }
 * }}}
 */
trait LambdaFunction extends Function {

  def call(args: List[Expression]): FunctionCall

}