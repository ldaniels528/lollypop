package com.qwery.runtime

import com.qwery.die
import com.qwery.language.models.Expression.implicits.LifestyleExpressions
import com.qwery.language.models._
import com.qwery.language.{QweryUniverse, dieIllegalType, dieNoSuchColumnOrVariable, dieNoSuchFunction, dieObjectIsNotADatabaseDevice, dieUnsupportedType}
import com.qwery.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.qwery.runtime.RuntimeClass.implicits.RuntimeClassConstructorSugar
import com.qwery.runtime.Scope._
import com.qwery.runtime.datatypes.Inferences.fromValue
import com.qwery.runtime.datatypes._
import com.qwery.runtime.devices.RecordCollectionZoo.MapToRow
import com.qwery.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.qwery.runtime.devices._
import com.qwery.runtime.instructions.functions.{DataTypeConstructor, InternalFunctionCall}
import com.qwery.runtime.instructions.invocables.EOL
import com.qwery.runtime.instructions.queryables.TableVariableRef
import com.qwery.util.LogUtil
import com.qwery.util.OptionHelper.OptionEnrichment
import com.qwery.util.ResourceHelper._
import com.qwery.util.StringRenderHelper.StringRenderer

import java.io.{PrintWriter, StringWriter}
import java.lang.reflect.Method
import scala.language.{implicitConversions, reflectiveCalls}
import scala.util.{Failure, Success, Try}

/**
 * Represents an immutable scope implementation
 */
case class DefaultScope(superScope: Option[Scope] = None,
                        aliasedRows: Map[String, Row] = Map(),
                        aliasedSources: Map[String, RowCollection with CursorSupport] = Map(),
                        universe: QweryUniverse = QweryUniverse(),
                        currentRow: Option[Row] = None,
                        implicitMethods: List[ImplicitMethod] = Nil,
                        imports: Map[String, String] = Map(),
                        returned: Boolean = false,
                        serverMode: Boolean = false,
                        observables: List[Observable] = Nil,
                        observed: Boolean = false,
                        references: Map[DatabaseObjectNS, AnyRef] = Map(),
                        tracers: List[TraceEventHandler] = Nil,
                        valueReferences: Map[String, ValueReference] = Map()) extends Scope {

  private val specialVariables: Map[String, () => Any] = Map(
    "__scope__" -> { () => this },
    "__imports__" -> { () => getImports },
    "__implicit_imports__" -> { () =>
      getImplicitMethods.map(m => m.method.getDeclaringClass.getName -> m.method.getName).groupBy(_._1)
        .map { case (k, values) => k -> values.map(_._2).toSet }
    },
    "__loaded__" -> { () => getUniverse.system.getReferencedEntities },
    __namespace__ -> { () => (apply(__database__) || DEFAULT_DATABASE) + "." + (apply(__schema__) || DEFAULT_SCHEMA) },
    "__resources__" -> { () => ResourceManager.getResources },
    "__userHome__" -> { () => scala.util.Properties.userHome },
    "__userName__" -> { () => scala.util.Properties.userName },
    "__version__" -> { () => version }
  )

  override def ++(that: Scope): Scope = {
    DefaultScope(
      superScope = this.getSuperScope,
      aliasedRows = this.getAliasedRows ++ that.getAliasedRows,
      aliasedSources = this.getAliasedSources ++ that.getAliasedSources,
      universe = this.getUniverse,
      currentRow = this.getCurrentRow ?? that.getCurrentRow,
      implicitMethods = this.getImplicitMethods ::: that.getImplicitMethods,
      imports = this.getImports ++ that.getImports,
      returned = this.isReturned,
      observables = this.getObservables ++ that.getObservables,
      observed = this.isObserved || that.isObserved,
      references = this.getReferences ++ that.getReferences,
      tracers = this.getTracers,
      valueReferences = this.getValueReferences ++ that.getValueReferences)
  }

  override def apply(path: String): Option[Any] = resolve(path, isRequired = true)

  override def getAliasedRows: Map[String, Row] = (superScope.map(_.getAliasedRows) || Map.empty) ++ aliasedRows

  override def getAliasedSources: Map[String, RowCollection with CursorSupport] = {
    (superScope.map(_.getAliasedSources) || Map.empty) ++ aliasedSources
  }

  override def getCompiler: QweryCompiler = QweryCompiler(getUniverse)

  override def getDatabase: Option[String] = resolveAs(__database__)

  override def isDataSource(name: String): Boolean = getAliasedSources.contains(name)

  override def getDataSourceValue(name: String, property: String): Option[Any] = {
    for {
      collection <- getAliasedSources.get(name)
      row = collection.get
      field <- row.getField(property)
    } yield field.value
  }

  override def isDefined(name: String): Boolean = {
    name == ROWID_NAME ||
      getAliasedSources.contains(name) ||
      getImports.contains(name) ||
      getValueReferences.contains(name) ||
      getCurrentRow.exists(_.contains(name))
  }

  override def getUniverse: QweryUniverse = universe

  override def getCurrentRow: Option[Row] = currentRow ?? superScope.flatMap(_.getCurrentRow)

  override def getImports: Map[String, String] = (superScope.map(_.getImports) || Map.empty) ++ imports

  override def getImplicitMethods: List[ImplicitMethod] = implicitMethods

  override def withImplicitMethods(method: ImplicitMethod): DefaultScope = {
    this.copy(implicitMethods = method :: implicitMethods)
  }

  override def invokeImplicitMethod(instance: Any, methodName: String, args: Any*): AnyRef = {
    val registration_? = getImplicitMethods.find(r => r.method.getName == methodName && r.params.length == args.length)
    registration_? match {
      case Some(r) => r.method.invoke(r.constructor.newInstance(instance), args: _*)
      case None => dieNoSuchFunction(methodName)
    }
  }

  override def isImplicitMethod(methodName: String, args: Any*): Boolean = {
    getImplicitMethods.exists(r => r.method.getName == methodName && r.params.length == args.length)
  }

  override def importImplicitClass(implicitClass: Class[_]): Scope = {
    // find the single argument constructor
    implicitClass.getConstructors.find(_.getParameterCount == 1) match {
      case Some(constructor) =>
        // get the declared methods
        val declaredMethods: Seq[Method] = {
          val restricted = Seq("equals", "hashCode")
          implicitClass.getDeclaredMethods.filterNot(m => restricted.contains(m.getName))
        }

        // import all declared methods
        declaredMethods.foldLeft(this) { case (agg, method) =>
          val params = method.getParameters.map { p =>
            Parameter(name = p.getName, `type` = Inferences.fromClass(p.getType).toColumnType)
          }
          agg.withImplicitMethods(ImplicitMethod(constructor, method, params, method.getReturnType))
        }
      case None => this
    }
  }

  override def getMemoryObject(ref: DatabaseObjectRef): Option[AnyRef] = getReferences.get(ref.toNS(this))

  override def getObservables: List[Observable] = observables ::: superScope.toList.flatMap(_.getObservables)

  override def isObserved: Boolean = observed || superScope.exists(_.isObserved)

  override def getReferences: Map[DatabaseObjectNS, AnyRef] = {
    (superScope.map(_.getReferences) || Map.empty) ++ references
  }

  override def isReturned: Boolean = returned || superScope.exists(_.isReturned)

  override def getRowCollection(ref: DatabaseObjectRef): RowCollection = {
    ref.realize(this) match {
      case ns: DatabaseObjectNS if ns.name.startsWith("@@") => resolveTableVariable(ns.name.drop(2))
      case DatabaseObjectRef.InnerTable(TableVariableRef(baseName), _) => resolveTableVariable(baseName)
      case tvr: TableVariableRef => resolveTableVariable(tvr.name)
      case dor =>
        getUniverse.getReferencedEntity(dor.toNS(this))(this) match {
          case rc: RowCollection => rc
          case _ => dieObjectIsNotADatabaseDevice(dor)
        }
    }
  }

  override def getSchema: Option[String] = resolveAs(__schema__)

  override def getSuperScope: Option[Scope] = superScope

  override def getTableVariable(name: String): Option[TableVariableRef] = {
    (getValueReferences.get(name).map(_.value(this)) ?? specialVariables.get(name).map(_.apply())).collect {
      case _: RowCollection => name
      case _: IBLOB => name
    } map TableVariableRef.apply
  }

  override def getTracers: List[TraceEventHandler] = tracers ::: superScope.toList.flatMap(_.getTracers)

  override def getVariable(name: String): Option[ValueReference] = getValueReferences.get(name)

  override def getValueReferences: Map[String, ValueReference] = {
    (superScope.map(_.getValueReferences) || Map.empty) ++ valueReferences
  }

  override def removeVariable(name: String): Scope = {
    valueReferences.get(name) foreach { vr =>
      vr.value(this) match {
        case ac: AutoCloseable =>
          LogUtil(this).info(s"calling $name.close()")
          ac.close()
        case _ =>
      }
    }
    this.copy(valueReferences = valueReferences.removed(name))
  }

  override def reset(): Scope = withReturned(isReturned = false)

  override def resolve(path: String, isRequired: Boolean = false): Option[Any] = {
    val result = path match {
      case name if name == ROWID_NAME => getCurrentRow.map(_.id)
      case name if getAliasedSources contains name => getAliasedSources.get(name)
      case name =>
        getCurrentRow.flatMap(_.getField(name)) match {
          case Some(field) => field.value
          case None =>
            getValueReferences.get(name) match {
              case Some(variable) => Option(variable.value(this))
              case None if name.startsWith("__") => specialVariables.get(name).map(_.apply())
              case None => if (isRequired) dieNoSuchColumnOrVariable(name) else None
            }
        }
    }
    result
  }

  override def resolveAs[A](path: String): Option[A] = resolve(path).flatMap(safeCast[A])

  override def resolveAny(name: String, args: List[Expression]): Any = {
    // first: determine whether it's a row function
    resolveInternalFunctionCall(name, args) match {
      case Some(fx) => fx
      case None =>
        // next: determine whether it's a lambda function
        resolve(name) match {
          case Some(fx) => fx
          case None =>
            // next: determine if it's a type constructor
            getUniverse.dataTypeParsers.find(_.synonyms.contains(name)) match {
              case Some(fx: ConstructorSupport[_]) => DataTypeConstructor(fx)
              case Some(fx) => fx
              case None =>
                // next: determine whether it's a stored function
                resolveStoredFunction(ref = DatabaseObjectRef(name)) match {
                  case (_, Some(fx)) => fx
                  case (_, None) =>
                    // finally: is it a stored data type?
                    Try(DataType.load(name.ct)(this)) match {
                      case Success(fx: ConstructorSupport[_]) => DataTypeConstructor(fx)
                      case Success(fx) => die(s"${fx.getClass.getSimpleName} is not a function")
                      case Failure(_) => dieNoSuchFunction(name)
                    }
                }
            }
        }
    }
  }

  override def resolveInternalFunctionCall(functionName: String, args: List[Expression]): Option[FunctionCall] = {
    for {
      parser <- getUniverse.functionCallParsers.find(_.name == functionName)
      fx <- parser.getFunctionCall(args)
    } yield fx
  }

  override def resolveReferenceName(instruction: Instruction): String = instruction match {
    case FieldRef(name) => name
    case VariableRef(name) => name
    case f: InternalFunctionCall => f.functionName
    case n: NamedExpression => n.name
    case other => dieIllegalType(other)
  }

  private def resolveStoredFunction(ref: DatabaseObjectRef): (Scope, Option[TypicalFunction]) = {
    val scope: Scope = this
    val ns = ref.toNS(scope)
    getReferences.get(ns) match {
      case Some(fx: TypicalFunction) => scope -> Some(fx)
      case Some(x) => die(s"Type mismatch: '$x' is not a user function")
      case None =>
        if (!ns.configFile.exists()) scope -> None
        else {
          Try(DatabaseManagementSystem.readDurableFunction(ref.toNS(this))(scope)) match {
            case Success(fx) =>
              val scope1 = this.withReference(ns, fx)
              scope1 -> Option(fx)
            case Failure(e) =>
              getUniverse.system.stdErr.writer.println(s"${ref.toSQL}: ${e.getMessage}")
              scope -> None
          }
        }
    }
  }

  override def resolveValueReferenceName(instruction: Instruction): String = instruction match {
    case FieldRef(name) => name
    case VariableRef(name) => name
    case other => dieIllegalType(other)
  }

  override def resolveTableVariable(name: String): RowCollection = {
    getValueReferences.get(name) match {
      case Some(variable) =>
        variable.value(this) match {
          case null => die(s"Variable '$name' is not a table")
          case collection: RowCollection => collection
          case blob: IBLOB => FileRowCollection(blob.ns)
          case other => die(s"Variable '$name' (${other.getClass.getName}) is not a table")
        }
      case None => specialVariables.getOrElse(name, die(s"Variable '$name' was not found"))() match {
        case rc: RowCollection => rc
        case other => dieUnsupportedType(other)
      }
    }
  }

  override def setVariable(name: String, instruction: Instruction): Scope = {
    val (scopeA, _, valueA) = QweryVM.execute(this, instruction)
    scopeA.setVariable(name, valueA)
  }

  override def setVariable(name: String, value: Any): Scope = {
    val scopeA = getValueReferences.get(name) match {
      case Some(variable) =>
        implicit val _scope: Scope = this
        variable.value = value
        _scope.withVariable(variable)
      case None => withVariable(name, value)
    }
    scopeA
  }

  override def toRowCollection: RowCollection = {
    val maxLen = 30

    // include the current row's __id
    val rowsCR_Id = getCurrentRow.map(_.id).toList map { id =>
      Map("name" -> ROWID_NAME, "kind" -> Int64Type.toSQL, "value" -> id)
    }

    // include the table columns
    val rowsCR_cols = (getCurrentRow.toList.flatMap(_.columns) zip getCurrentRow.toList.flatMap(_.fields)) map { case (column, field) =>
      Map(
        "name" -> field.name,
        "kind" -> column.`type`.toJavaType(hasNulls = true).getJavaTypeName,
        "value" -> field.value.map(_.renderAsJson).map(v => if (v.length <= maxLen) v else "...").getOrElse(""))
    }

    // include all variables
    val rowsVars = getValueReferences
      .filterNot { case (name, _) => name.startsWith("__") }
      .map { case (_, variable) =>
        Map(
          "name" -> variable.name,
          "kind" -> (variable.value(this) match {
            case null | None => ""
            case _: RowCollection => "Table"
            case v => v.getClass.getJavaTypeName
          }),
          "value" -> (variable.value(this) match {
            case array: Array[_] => array.renderAsJson
            case r: RowCollection => s"Table(${r.columns.map(_.toSQL).mkString(", ")})"
            case i: Instruction => i.toSQL
            case x => x.renderAsJson
          }).take(8192))
      }
      .filterNot(_.exists { case ("kind", "Class") => true; case _ => false })
      .toList

    // write the rows
    implicit val out: RowCollection = createQueryResultTable(returnType.columns)
    for (mapping <- rowsCR_Id ::: rowsCR_cols ::: rowsVars) out.insert(mapping.toRow)
    out
  }

  override def withAliasedRows(aliasedRows: Map[String, Row]): Scope = {
    this.copy(aliasedRows = this.aliasedRows ++ aliasedRows)
  }

  override def withAliasedSources(aliasedSources: Map[String, RowCollection with CursorSupport]): Scope = {
    this.copy(aliasedSources = this.aliasedSources ++ aliasedSources)
  }

  override def withArguments[A <: ParameterLike](params: Seq[A], args: Seq[Any]): Scope = {
    val values = params.zipWithIndex.map { case (param, n) => if (args.length > n) args(n) else param.defaultValue.map(QweryVM.execute(this, _)._3).orNull }
    withArguments(keyValues = params.map(_.name) zip values)
  }

  override def withArguments(keyValues: Seq[(String, Any)]): Scope = {
    keyValues.foldLeft[Scope](this) { case (scope, (name, value)) => scope.withVariable(name, value, isReadOnly = true) }
  }

  override def withCurrentRow(row: Option[Row]): Scope = this.copy(currentRow = row)

  override def withDatabase(databaseName: String): Scope = withVariable(__database__, databaseName)

  override def withDataSource(name: String, source: RowCollection with CursorSupport): Scope = {
    this.withAliasedSources(aliasedSources = Map(name -> source))
  }

  override def withEnvironment(ctx: QweryUniverse): Scope = this.copy(universe = ctx)

  override def withImports(imports: Map[String, String]): Scope = this.copy(imports = this.imports ++ imports)

  override def withObservable(observable: Observable): Scope = this.copy(observables = observable :: observables)

  override def withObserved(observed: Boolean): Scope = this.copy(observed = observed)

  override def withParameters[A <: ParameterLike](params: Seq[A], args: Seq[Instruction]): Scope = {
    val ops = params.zipWithIndex.map { case (param, n) => if (args.length > n) args(n) else param.defaultValue getOrElse EOL }
    val values = ops.map(op => QweryVM.execute(this, op)._3)
    withArguments(keyValues = params.map(_.name) zip values)
  }

  override def withReference(ref: DatabaseObjectNS, referenced: AnyRef): Scope = {
    this.copy(references = references ++ Map(ref -> referenced))
  }

  override def withReturned(isReturned: Boolean): Scope = this.copy(returned = isReturned)

  override def withSchema(schemaName: String): Scope = withVariable(__schema__, Option(schemaName))

  override def withThrowable(e: Throwable): Scope = {
    new StringWriter(4096) use { sw =>
      new PrintWriter(sw).use(e.printStackTrace)
      withVariable(name = "__last_error", value = Option(sw.toString))
    }
  }

  override def withTrace(f: TraceEventHandler): Scope = this.copy(tracers = f :: tracers)

  override def withVariable(ref: ValueReference): Scope = {
    this.copy(valueReferences = valueReferences ++ Map(ref.name -> ref))
  }

  override def withVariable(name: String, code: Instruction, isReadOnly: Boolean): Scope = code match {
    case udf: TypicalFunction =>
      withVariable(name, value = udf.asInstanceOf[Any], isReadOnly)
    case op =>
      val (_, _, result) = QweryVM.execute(this, op)
      this.withVariable(name, value = result, isReadOnly)
  }

  override def withVariable(name: String, codec: LambdaFunction, initialValue: Instruction): Scope = {
    this.withVariable(EncodedVariable(name, codec, initialValue))
  }

  override def withVariable(name: String, value: Any, isReadOnly: Boolean = false): Scope = {
    withVariable(name, `type` = fromValue(value), value = value, isReadOnly = isReadOnly)
  }

  override def withVariable(name: String, `type`: DataType, value: Any, isReadOnly: Boolean): Scope = {
    val variable = Variable(name, `type`, initialValue = `type`.convert(value), isReadOnly = isReadOnly)
    this.withVariable(variable)
  }

}