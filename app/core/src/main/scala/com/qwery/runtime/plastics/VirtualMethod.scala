package com.qwery.runtime.plastics

import com.qwery.runtime.instructions.functions.NamedFunction
import com.qwery.runtime.plastics.RuntimeClass.implicits.RuntimeClassConstructorSugar

/**
 * Represents a Virtual Method
 */
sealed trait VirtualMethod {

  /**
   * @return the [[NamedFunction function]] that represents the virtual method
   */
  def method: NamedFunction

  /**
   * Indicates whether the value is a compatible host for the virtual method
   * @param value the instance to test
   * @return true, if the value is a compatible host for the virtual method
   */
  def isMatch(value: Any): Boolean

}

/**
 * Virtual Method Companion
 */
object VirtualMethod {

  def apply(f: Any => Boolean, fx: NamedFunction): VirtualMethod = VirtualMethodByInstance(f, fx)

  def apply(`class`: Class[_], fx: NamedFunction): VirtualMethod = VirtualMethodByClass(`class`, fx)

  /**
   * Represents a Virtual Method that matches a host by instance
   * @param f      the value test function
   * @param method the virtual method executable code
   */
  private case class VirtualMethodByInstance(f: Any => Boolean, method: NamedFunction) extends VirtualMethod {
    override def isMatch(value: Any): Boolean = f(value)
  }

  /**
   * Represents a Virtual Method that matches a host by class
   * @param `class` the JVM class the method is bound to
   * @param method  the virtual method executable code
   */
  private case class VirtualMethodByClass(`class`: Class[_], method: NamedFunction) extends VirtualMethod {
    override def isMatch(value: Any): Boolean = value match {
      case c: Class[_] => c == `class` | c.isDescendantOf(`class`) | `class`.isDescendantOf(c)
      case v => isMatch(v.getClass)
    }
  }

}


