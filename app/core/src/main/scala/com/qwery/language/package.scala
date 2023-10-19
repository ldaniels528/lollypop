package com.qwery

import com.qwery.language.models.Instruction
import com.qwery.runtime.DatabaseObjectRef
import com.qwery.util.StringHelper.StringEnrichment
import com.qwery.util.StringRenderHelper.StringRenderer

import java.io.File

/**
 * Language package object
 */
package object language {

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //      MANAGED EXCEPTIONS
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @inline
  def dieArgumentMismatch(args: Int, minArgs: Int, maxArgs: Int): Nothing = {
    die(message =
      if (minArgs == 0 && maxArgs == 0) "No arguments were expected"
      else if (minArgs == maxArgs) s"Exactly $minArgs argument(s) expected"
      else if (args < minArgs) s"At least $minArgs argument(s) expected"
      else "Too many arguments"
    )
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //      CONVENIENCE METHODS FOR EXCEPTIONS
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @inline def dieExpectedArray(): Nothing = die("An array value was expected")

  @inline def dieExpectedDate(v: Any): Nothing = die(s"A date-compatible value was expected '$v' (${v.getClass.getName})")

  @inline def dieExpectedJSONObject(v: Any): Nothing = die(s"A JSON object was expected '$v' (${v.getClass.getName})")

  @inline def diePointerExpected(value: Any): Nothing = die(s"Expected pointer but got '$value' (${Option(value).map(_.getClass.getSimpleName).orNull})")

  @inline def dieFileNotDirectory(f: File): Nothing = die(s"Path '${f.getCanonicalPath}' is not a directory")

  @inline def dieFileNotFound(ref: DatabaseObjectRef): Nothing = die(s"No file or directory could be determine for table '${ref.toSQL}'")

  @inline def dieFunctionNotCompilable(ref: DatabaseObjectRef): Nothing = die(s"Function '${ref.toSQL}' could not be compiled")

  @inline def dieIllegalObjectRef(path: String): Nothing = die(s"Illegal object reference '$path'")

  @inline def dieIllegalType(value: Any): Nothing = die(s"Unexpected type returned '$value' (${Option(value).map(_.getClass.getSimpleName).orNull})")

  @inline def dieNoResultSet(): Nothing = die("Result set expected")

  @inline def dieNoSuchColumn(name: String): Nothing = die(s"Column '$name' not found")

  @inline def dieNoSuchColumnOrVariable(name: String): Nothing = die(s"Column or variable '$name' not found")

  @inline def dieNoSuchConstructor(name: String): Nothing = die(s"No suitable constructor found for class '$name'")

  @inline def dieNoSuchFunction(name: String): Nothing = die(s"Function '$name' was not found")

  @inline def dieObjectAlreadyExists(ref: DatabaseObjectRef): Nothing = die(s"Object '${ref.toSQL}' already exists")

  @inline def dieObjectIsNotADatabaseDevice(ref: DatabaseObjectRef): Nothing = die(s"Object '$ref' is not a database device")

  @inline def dieObjectIsNotAFunction(ref: DatabaseObjectRef): Nothing = die(s"Object '$ref' is not an external function")

  @inline def dieObjectIsNotAUserFunction(ref: DatabaseObjectRef): Nothing = die(s"Object '${ref.toSQL}' is not a user function")

  @inline def dieTableIsReadOnly(): Nothing = die("Table is read-only")

  @inline def dieUnsupportedConversion(v: Any, typeName: String): Nothing = {
    die(s"Conversion from '${v.renderAsJson.limit(30)}' (${v.getClass.getName}) to $typeName is not supported")
  }

  @inline def dieUnsupportedEntity(i: Instruction, entityName: String): Nothing = die(s"Unsupported $entityName: ${i.toSQL} (${i.getClass.getName})")

  @inline def dieUnsupportedType(v: Any): Nothing = die(s"Unsupported type $v (${v.getClass.getName})")

  /**
   * Token Stream Exceptions
   * @param ts the given [[TokenStream]]
   */
  final implicit class TokenStreamExceptions(val ts: TokenStream) extends AnyVal {

    @inline def die(message: => String, cause: Throwable = null): Nothing = throw SyntaxException(message, ts, cause)

    @inline
    def dieArgumentMismatch(args: Int, minArgs: Int, maxArgs: Int): Nothing = {
      die(message =
        if (minArgs == 0 && maxArgs == 0) "No arguments were expected"
        else if (minArgs == maxArgs) s"Exactly $minArgs argument(s) expected"
        else if (args < minArgs) s"At least $minArgs argument(s) expected"
        else "Too many arguments"
      )
    }

    @inline def dieExpectedAlteration(): Nothing = die("At least one alteration was expected")

    @inline def dieExpectedArray(): Nothing = die("Array value expected")

    @inline def dieExpectedBoolean(): Nothing = die("Boolean expression expected")

    @inline def dieExpectedClassType(): Nothing = die("Class type identifier expected")

    @inline def dieExpectedCondition(): Nothing = die("A condition was expected")

    @inline def dieExpectedConditionOrExpression(): Nothing = die("Expression or condition expected")

    @inline def dieExpectedDataType(): Nothing = die("Data type identifier expected")

    @inline def dieExpectedEntityRef(): Nothing = die("A database object reference was expected")

    @inline def dieExpectedEnumIdentifier(): Nothing = die("Enum identifier expected")

    @inline def dieExpectedExpression(): Nothing = die("An expression was expected")

    @inline def dieExpectedField(): Nothing = die("Field identifier expected")

    @inline def dieExpectedIdentifier(): Nothing = die("Identifier expected")

    @inline def dieExpectedIfExists(): Nothing = die("if exists expected")

    @inline def dieExpectedIfNotExists(): Nothing = die("if not exists expected")

    @inline def dieExpectedParameter(): Nothing = die("An parameter was expected")

    @inline def dieExpectedKeywords(keywords: String*): Nothing = {
      val keywordString = keywords match {
        case Seq(oneWord) => oneWord
        case words =>
          val (head, tail) = (words.init, words.last)
          head.mkString(", ") + " or " + tail
      }
      die(s"Expected keyword $keywordString")
    }

    @inline def dieExpectedNumeric(): Nothing = die("Numeric value expected")

    @inline def dieExpectedObject(): Nothing = die(s"Object type (e.g. table) expected")

    @inline def dieExpectedOneArgument(): Nothing = die("A single class argument was expected")

    @inline def dieExpectedOrderedColumn(): Nothing = die("Order column definition expected")

    @inline def dieExpectedModifiable(): Nothing = die("An operation was expected")

    @inline def dieExpectedQueryable(): Nothing = die("A queryable was expected")

    @inline def dieExpectedInvokable(): Nothing = die("A statement was expected")

    @inline def dieExpectedScopeMutation(i: Instruction): Nothing = i.die("A scope mutation instruction was expected")

    @inline def dieExpectedTableNotation(): Nothing =
      die("""Table notation expected (e.g. "public"."stocks" or "portfolio.public.stocks" or `Months of the Year`)""")

    @inline def dieExpectedVariable(): Nothing = die("Variable expected")

    @inline def dieIllegalIdentifier(): Nothing = die("Illegal identifier")

    @inline def dieIllegalVariableName(): Nothing = die("Invalid variable name")

    @inline def dieIllegalType(value: Any): Nothing = die(s"Unexpected type returned '$value' (${Option(value).map(_.getClass.getSimpleName).orNull})")

    @inline def dieMultipleColumnsNotSupported(): Nothing = die("Multiple columns are not supported")

    @inline def diePatternNotMatched(): Nothing = die("Pattern did not match")

    @inline def dieScalarVariableIncompatibleWithRowSets(): Nothing = die("Scalar variable references are not compatible with row sets")

    @inline def dieTemplateError(values: Seq[String]): Nothing = die(s"Unexpected template error: ${values.mkString(", ")}")

    @inline def dieUnrecognizedCommand(): Nothing = die("Unrecognized command")

  }

}
