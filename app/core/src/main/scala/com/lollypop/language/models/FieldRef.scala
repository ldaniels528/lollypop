package com.lollypop.language.models

import com.lollypop.die
import com.lollypop.runtime.instructions.expressions.{BasicFieldRef, JoinFieldRef}

/**
  * Represents a SQL field reference
  * @author lawrence.daniels@gmail.com
  */
trait FieldRef extends IdentifierRef

/**
  * Field Reference Companion
  * @author lawrence.daniels@gmail.com
  */
object FieldRef {

  /**
    * Returns a new field implementation
    * @param descriptor the name (e.g. "customerId") or descriptor ("A.customerId") of the field reference
    * @return the [[FieldRef field reference]]
    */
  def apply(descriptor: String): FieldRef = descriptor.split('.').toList match {
    case "*" :: Nil => AllFields
    case name :: Nil => BasicFieldRef(name)
    case tableAlias :: name :: Nil => JoinFieldRef(tableAlias, name)
    case _ => die(s"Invalid field descriptor '$descriptor'")
  }

  def unapply(field: FieldRef): Option[String] = Option(field.name)

}
