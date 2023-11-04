package com.lollypop.runtime

import com.lollypop.language._
import com.lollypop.language.models._
import com.lollypop.runtime.Scope._
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices._
import com.lollypop.runtime.instructions.queryables.TableVariableRef

import java.io._
import java.lang.reflect.{Constructor, Method}
import scala.language.{implicitConversions, postfixOps}
import scala.util.Try

/**
 * Represents a scope structure
 */
trait Scope {

  def ++(that: Scope): Scope

  /**
   * Retrieves a value by name from the scope
   * @param path the name of the desired field/attribute (e.g. "employee.name.first")
   * @return the option of a value
   */
  def apply(path: String): Option[Any]

  def getAliasedRows: Map[String, Row]

  def getAliasedSources: Map[String, RowCollection with CursorSupport]

  def getCompiler: LollypopCompiler

  def getCurrentRow: Option[Row]

  def getDatabase: Option[String]

  def isDataSource(name: String): Boolean

  def getDataSourceValue(name: String, property: String): Option[Any]

  def isDefined(name: String): Boolean

  def getUniverse: LollypopUniverse

  def getImports: Map[String, String]

  def getImplicitMethods: List[ImplicitMethod]

  def withImplicitMethods(method: ImplicitMethod): Scope

  /**
   * Invokes a method of an implicit class
   * @param instance   the host instance
   * @param methodName the name of the method of the implicit class
   * @param args       the method arguments
   * @return the results of the invocation
   * @example {{{
   * import implicit "com.lollypop.util.StringRenderHelper$StringRenderer"
   * 'Hello'.renderAsJson()
   * }}}
   */
  def invokeImplicitMethod(instance: Any, methodName: String, args: Any*): AnyRef

  /**
   * Indicates whether the method is of an imported implicit class
   * @param methodName the method name for which to search
   * @param args       the method arguments
   * @return true, if the method is of an imported implicit class
   */
  def isImplicitMethod(methodName: String, args: Any*): Boolean

  /**
   * Imports all methods of an implicit class
   * @param implicitClass the implicit class
   * @return a new augmented [[Scope scope]]
   * @example {{{
   * import implicit "com.lollypop.util.StringRenderHelper$StringRenderer"
   * }}}
   */
  def importImplicitClass(implicitClass: Class[_]): Scope

  def getMemoryObject(ref: DatabaseObjectRef): Option[AnyRef]

  def getObservables: List[Observable]

  def isObserved: Boolean

  def getReferences: Map[DatabaseObjectNS, AnyRef]

  def isReturned: Boolean

  /**
   * Returns a database device by reference
   * @param ref the [[DatabaseObjectRef object reference]]
   * @return the [[RowCollection storage device]]
   */
  def getRowCollection(ref: DatabaseObjectRef): RowCollection

  def getSchema: Option[String]

  def getSuperScope: Option[Scope]

  /**
   * Attempts to retrieves the datasource for a table variable by name
   * @param name the table variable name
   * @return the option of a [[TableVariableRef datasource]] representing the variable
   */
  def getTableVariable(name: String): Option[TableVariableRef]

  def getTracers: List[TraceEventHandler]

  def getValueReferences: Map[String, ValueReference]

  def getVariable(name: String): Option[ValueReference]

  /**
   * Removes a variable from the scope
   * @param name the  name of the variable to remove
   */
  def removeVariable(name: String): Scope

  def reset(): Scope

  /**
   * Retrieves a value by name from the scope
   * @param path       the name of the desired field/attribute (e.g. "employee.name.first")
   * @param isRequired indicates that a reference must exist
   * @return the option of a value
   */
  def resolve(path: String, isRequired: Boolean = false): Option[Any]

  /**
   * Retrieves a value by name from the scope
   * @param path the name of the desired field/attribute (e.g. "employee.name.first")
   * @return the option of a value
   */
  def resolveAs[A](path: String): Option[A]

  /**
   * Attempts to resolve a function via its signature
   * @param name the name of the function
   * @param args the function arguments
   * @return the [[Try outcome]] of [[Functional functional expression]]
   */
  def resolveAny(name: String, args: List[Expression]): Any

  def resolveInternalFunctionCall(functionName: String, args: List[Expression]): Option[FunctionCall]

  def resolveReferenceName(instruction: Instruction): String

  def resolveValueReferenceName(instruction: Instruction): String

  /**
   * Retrieves the datasource for a table variable by name
   * @param name the table variable name
   * @return the [[RowCollection datasource]] representing the variable
   */
  def resolveTableVariable(name: String): RowCollection

  /**
   * Sets a variable with the value of an evaluated instruction
   * @param name        the variable name
   * @param instruction the [[Instruction instruction]] to set
   */
  def setVariable(name: String, instruction: Instruction): Scope

  /**
   * Sets a variable's value
   * @param name  the variable name
   * @param value the value to set
   */
  def setVariable(name: String, value: Any): Scope

  //////////////////////////////////////////////////////////////////////////////////
  //    STANDARD ERROR / INPUT / OUTPUT
  //////////////////////////////////////////////////////////////////////////////////

  def show(label: String): List[String] = {
    s"$label (${this.getClass.getSimpleName})" :: toRowCollection.tabulate()
  }

  def stdErr: PrintStream = getUniverse.system.stdErr.writer

  def stdIn: BufferedReader = getUniverse.system.stdIn.reader

  def stdOut: PrintStream = getUniverse.system.stdOut.writer

  //////////////////////////////////////////////////////////////////////////////////
  //    Accessors and Builders
  //////////////////////////////////////////////////////////////////////////////////

  def withAliasedRows(aliasedRows: Map[String, Row]): Scope

  def withAliasedSources(aliasedSources: Map[String, RowCollection with CursorSupport]): Scope

  def withArguments[A <: ParameterLike](params: Seq[A], args: Seq[Any]): Scope

  def withArguments(keyValues: Seq[(String, Any)]): Scope

  def withCurrentRow(row: Option[Row]): Scope

  def withDatabase(databaseName: String): Scope

  def withDataSource(name: String, source: RowCollection with CursorSupport): Scope

  def withEnvironment(ctx: LollypopUniverse): Scope

  def withImports(imports: Map[String, String]): Scope

  def withObservable(observable: Observable): Scope

  def withObserved(observed: Boolean): Scope

  def withParameters[A <: ParameterLike](params: Seq[A], args: Seq[Instruction]): Scope

  def withReference(ref: DatabaseObjectNS, referenced: AnyRef): Scope

  def withReturned(isReturned: Boolean): Scope

  def withSchema(schemaName: String): Scope

  def withThrowable(e: Throwable): Scope

  def withTrace(f: TraceEventHandler): Scope

  /**
   * Attaches a value reference (e.g. variable) to the scope
   * @param ref the [[ValueReference value reference]] to add
   */
  def withVariable(ref: ValueReference): Scope

  /**
   * Sets a variable's value
   * @param name       the variable name
   * @param code       the [[Instruction initial value]] to set
   * @param isReadOnly indicates whether the variable is immutable (read-only)
   */
  def withVariable(name: String, code: Instruction, isReadOnly: Boolean): Scope

  /**
   * Sets a variable's value having a codec function
   * @param name         the variable name
   * @param codec        the [[LambdaFunction codec]]
   * @param initialValue the [[Instruction initial value]] to set
   */
  def withVariable(name: String, codec: LambdaFunction, initialValue: Instruction): Scope

  /**
   * Sets a variable's value
   * @param name       the variable name
   * @param value      the value to set
   * @param isReadOnly indicates whether the variable is immutable (read-only)
   */
  def withVariable(name: String, value: Any, isReadOnly: Boolean = false): Scope

  /**
   * Sets a variable's value
   * @param name       the variable name
   * @param `type`     the [[DataType data type]]
   * @param value      the value to set
   * @param isReadOnly indicates whether the variable is immutable (read-only)
   */
  def withVariable(name: String, `type`: DataType, value: Any, isReadOnly: Boolean): Scope

  def toMap: Map[String, Any]

  def toRowCollection: RowCollection

  //////////////////////////////////////////////////////////////////////////////////
  //    Print I/O Streams
  //////////////////////////////////////////////////////////////////////////////////

  def debug(s: => String): Unit

  def info(s: => String): Unit

  def warn(s: => String): Unit

  def error(s: => String): Unit
  
}

/**
 * Scope Companion
 */
object Scope {

  type TraceEventHandler = (Instruction, Scope, Any, Double) => Unit

  /**
   * Creates a new root scope
   * @return the new [[Scope scope]]
   */
  def apply(): Scope = {
    LollypopUniverse().createRootScope()
  }

  /**
   * Creates a new local scope
   * @param parentScope the parent [[Scope scope]]
   * @return the new [[Scope scope]]
   */
  def apply(parentScope: Scope): Scope = {
    DefaultScope(superScope = Some(parentScope), universe = parentScope.getUniverse)
  }

  /**
   * Creates a new local scope having a predefined initial state
   * @param parentScope  the [[Scope scope]]
   * @param initialState the initial state of the [[Scope scope]]
   * @return the new [[Scope scope]]
   */
  def apply(parentScope: Scope, initialState: Map[String, Any]): Scope = {
    initialState.foldLeft[Scope](Scope(parentScope)) {
      case (scope, (name, value)) => scope.withVariable(name, value = Option(value))
    }
  }

  /**
   * Creates a new scope
   * @param ctx the [[LollypopUniverse compiler context]]
   * @return the new [[Scope scope]]
   */
  def apply(ctx: LollypopUniverse): Scope = ctx.createRootScope()

  case class ImplicitMethod(constructor: Constructor[_], method: Method, params: Seq[Parameter], returnType: Class[_])

}