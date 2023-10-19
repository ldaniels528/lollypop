package com.qwery.language.models

import com.qwery.runtime.Scope
import com.qwery.runtime.datatypes.{DataType, StringType}
import com.qwery.runtime.instructions.RuntimeInstruction
import qwery.io.IOCost

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

  final implicit class RichAtom(val expression: Instruction) extends AnyVal {
    def asAtom: Atom = expression match {
      case atom: Atom => atom
      case ref: IdentifierRef => Atom(ref.name)
      case Literal(value: String) => Atom(value)
      case x => x.dieIllegalType()
    }
  }

}