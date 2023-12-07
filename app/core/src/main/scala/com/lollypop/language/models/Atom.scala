package com.lollypop.language.models

import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.{DataType, StringType}
import com.lollypop.runtime.instructions.RuntimeInstruction
import lollypop.io.IOCost

import scala.language.implicitConversions

/**
 * Represents a generic named identifier
 * @param name the name of the identifier
 */
case class Atom(name: String) extends RuntimeInstruction with Literal with IdentifierRef {

  override def execute()(implicit scope: Scope): (Scope, IOCost, String) = (scope, IOCost.empty, name)

  override def returnType: DataType = StringType

  override def toSQL: String = if (name.forall(c => c.isLetterOrDigit || c == '_')) name else s"`$name`"

  //override def toString: String = toSQL

  override def value: String = name

}

object Atom {

  final implicit def stringToAtom(value: String): Atom = Atom(value)

}