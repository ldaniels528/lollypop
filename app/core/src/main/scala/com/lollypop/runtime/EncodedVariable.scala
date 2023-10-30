package com.lollypop.runtime

import com.lollypop.language.models.{Expression, Instruction, LambdaFunction}
import com.lollypop.runtime.datatypes.{DataType, Inferences}

import scala.collection.concurrent.TrieMap

/**
 * Represents an encoded variable
 * @param name         the name of the variable
 * @param codec        the [[LambdaFunction CODEC function]]
 * @param initialValue the initial [[Instruction value]]
 */
case class EncodedVariable(name: String, codec: LambdaFunction, initialValue: Instruction) extends ValueReference {
  private val cache = new TrieMap[Unit, Any]()

  def `type`(implicit scope: Scope): DataType = Inferences.fromValue(value(scope))

  def value(implicit scope: Scope): Any = cache.getOrElseUpdate((), eval(initialValue))

  def value_=(newValue: Any)(implicit scope: Scope): Unit = {
    value(scope)
    cache.replace((), newValue)
  }

  private def eval(value: Instruction)(implicit scope: Scope): Any = {
    val fx = codec.call(args = List(value).collect { case e: Expression => e })
    LollypopVM.execute(scope, fx)._3
  }

}