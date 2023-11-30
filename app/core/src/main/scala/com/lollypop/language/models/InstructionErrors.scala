package com.lollypop.language.models

import com.lollypop.LollypopException
import com.lollypop.runtime.ModelStringRenderer.ModelStringRendering
import com.lollypop.runtime.datatypes.DataType
import com.lollypop.runtime.errors._
import com.lollypop.util.StringHelper.StringEnrichment
import com.lollypop.util.StringRenderHelper.StringRenderer

/**
 * Instruction Errors
 */
trait InstructionErrors { self: Instruction =>

  def die(message: String): Nothing = throw LollypopException(s"$message $toMessage".trim)

  def die(message: String, cause: Throwable): Nothing = throw LollypopException(s"$message $toMessage".trim, cause)

  def dieArgumentMismatch(args: Int, minArgs: Int, maxArgs: Int): Nothing = {
    val message: String =
      if (minArgs == 0 && maxArgs == 0) "No arguments were expected"
      else if (minArgs == maxArgs) s"Exactly $minArgs argument(s) expected"
      else if (args < minArgs) s"At least $minArgs argument(s) expected"
      else "Too many arguments"
    throw new ArgumentMismatchError(s"$message $toMessage".trim)
  }

  def dieColumnIsNotTableType(name: String): Nothing = die(s"Column '$name' is not a Table type")

  def dieDivisionByZero(cause: String): Nothing = throw new DivisionByZeroError(s"$cause $toMessage".trim)

  def dieExpectedBoolean(): Nothing = die("Boolean expression expected")

  def dieExpectedBoolean(value: Any): Nothing = die(s"Boolean expression expected, but got |${value.asModelString}|")

  def dieExpectedExpression(): Nothing = die("Expression expected")

  def dieExpectedFunctionRef(): Nothing = die(s"A reference to a function was expected near ${self.toSQL}")

  def dieExpectedInterval(): Nothing = die("An interval expression was expected")

  def dieExpectedNumeric(): Nothing = die(s"A numeric expression was expected '${self.toSQL}'")

  def dieExpectedQueryable(): Nothing = die(s"A queryable source was expected '${self.toSQL}' (${self.getClass.getName})")

  def dieIllegalType(): Nothing = {
    die(s"Unexpected type returned near '${self.toSQL}'")
  }

  def dieIllegalType(value: Any): Nothing = {
    die(s"Unexpected type returned '${value.asModelString.limit(40)}' near '${self.toSQL}'")
  }

  def dieInterfaceParametersNotSupported(): Nothing = throw new InterfaceArgumentsNotSupportedError(self)

  def dieMissingClassName(): Nothing = die("No class name was specified")

  def dieMissingSource(): Nothing = die("No source specified")

  def dieNoSuchClass(name: String): Nothing = die(s"Invalid class name - $name")

  def dieNoSuchColumn(name: String): Nothing = die(s"Column '$name' not found")

  def dieNoSuchVariable(name: String): Nothing = die(s"Variable '$name' was not found")

  def dieNotImplemented(): Nothing = throw new NotYetImplementedError()

  def dieNotInnerTable(): Nothing = die(s"$self is not a reference to an inner-table")

  def dieObjectIsNotADataType(v: Any): Nothing = die(s"Class '${v.getClass.getName}' is not a '${classOf[DataType].getName}''")

  def dieObjectNoSupportForAliases(): Nothing = die(s"Object '${self.toSQL}' does not support aliases")

  def dieResourceNotAutoCloseable(resource: Any): Nothing = throw new ResourceNotAutoCloseableException(resource)

  def dieTypeLoadFail(e: Throwable): Nothing = die(s"Type '${self.toSQL}' could not be loaded: ${e.getMessage}", e)

  def dieTypeMissingDependency(e: Throwable): Nothing = die(s"Type '${self.toSQL}' failed due to a missing dependency: ${e.getMessage}", e)

  def dieUrlIsNull(): Nothing = dieXXXIsNull(name = "URL")

  def dieXXXIsNull(name: String): Nothing = die(s"$name is missing or null")

}
