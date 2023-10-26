package com.qwery.language

import com.qwery.language.QweryUniverse.{_classLoader, _dataTypeParsers, _languageParsers}
import com.qwery.language.instructions.Include
import com.qwery.language.models._
import com.qwery.runtime._
import com.qwery.runtime.datatypes._
import com.qwery.runtime.instructions.MacroLanguageParser
import com.qwery.runtime.instructions.conditions._
import com.qwery.runtime.instructions.expressions._
import com.qwery.runtime.instructions.expressions.aggregation._
import com.qwery.runtime.instructions.functions._
import com.qwery.runtime.instructions.infrastructure._
import com.qwery.runtime.instructions.invocables._
import com.qwery.runtime.instructions.jvm._
import com.qwery.runtime.instructions.operators._
import com.qwery.runtime.instructions.queryables._
import com.qwery.runtime.plastics.RuntimePlatform
import qwery.io._
import qwery.lang._

/**
 * Qwery Universe - repository for long-lived state
 * @param dataTypeParsers the [[DataTypeParser data type parser]]
 * @param languageParsers the [[LanguageParser language parser]]
 * @param classLoader     the [[DynamicClassLoader classloader]]
 * @param escapeCharacter the [[Char escape character]]
 * @param isServerMode    indicates whether STDERR, STDIN and STDOUT is to be buffered
 */
case class QweryUniverse(var dataTypeParsers: List[DataTypeParser] = _dataTypeParsers,
                         var languageParsers: List[LanguageParser] = _languageParsers,
                         var classLoader: DynamicClassLoader = _classLoader,
                         var escapeCharacter: Char = '`',
                         var isServerMode: Boolean = false) {

  // define the default compiler
  val compiler: QweryCompiler = QweryCompiler(this)

  // create the systems utility
  val system: OS = new OS(this)

  def createRootScope(): Scope = {
    DefaultScope(universe = this)
      .withVariable("π", Math.PI)
      .withVariable("OS", system)
      .withVariable("Random", qwery.lang.Random)
      .withVariable("err", system.stdErr.writer)
      .withVariable("out", system.stdOut.writer)
      .withVariable("stdin", system.stdIn.reader)
      .withImports(Map(Seq(
        classOf[BitArray], classOf[ComplexNumber], classOf[java.util.Date], classOf[IOCost],
        classOf[Matrix], classOf[Pointer], classOf[RowIDRange]
      ).map(c => c.getSimpleName -> c.getName): _*))
  }


  /**
   * Returns a database entity by namespace
   * @param ns    the [[DatabaseObjectNS object namespace]]
   * @param scope the implicit [[Scope scope]]
   * @return the [[AnyRef referenced entity]]
   */
  def getReferencedEntity(ns: DatabaseObjectNS)(implicit scope: Scope): Any = {
    import DatabaseManagementSystem._
    import DatabaseObjectConfig.implicits.RichDatabaseEntityConfig
    scope.getMemoryObject(ns) match {
      case Some(entity) => entity
      case None =>
        ResourceManager.getResourceOrElseUpdate(ns, {
          ns.getConfig match {
            case config if config.isExternalTable => readExternalTable(ns)
            case config if config.isFunction => readDurableFunction(ns)
            case config if config.isMACRO => readMACRO(ns)
            case config if config.isProcedure => readProcedure(ns)
            case config if config.isUserType => readUserType(ns)
            case config if config.isVirtualTable => readVirtualTable(ns)
            case _ => readPhysicalTable(ns)
          }
        })
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  //    MACROS
  //////////////////////////////////////////////////////////////////////////////

  def addMacro(model: Macro): Unit = MacroLanguageParser.registerMacro(model)

  //////////////////////////////////////////////////////////////////////////////
  //    LANGUAGE PARSERS
  //////////////////////////////////////////////////////////////////////////////

  def functionCallParsers: List[FunctionCallParser] = languageParsers.collect { case fp: FunctionCallParser => fp }

  def getColumnType(ts: TokenStream)(implicit compiler: SQLCompiler): Option[ColumnType] = matchType(ts) {
    case parser: ColumnTypeParser => parser.parseColumnType(ts)
  }

  def getConditionToConditionChain(ts: TokenStream, host: Condition)(implicit compiler: SQLCompiler): Option[Condition] = matchInstruction(ts) {
    case parser: ConditionToConditionPostParser => parser.parseConditionChain(ts, host)
  }

  def getConditionToExpressionChain(ts: TokenStream, host: Condition)(implicit compiler: SQLCompiler): Option[Expression] = matchInstruction(ts) {
    case parser: ConditionToExpressionPostParser => parser.parseConditionChain(ts, host)
  }

  def getDirective(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Instruction] = matchInstruction(ts) {
    case parser: DirectiveParser => parser.parseDirective(ts)
  }

  def getExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Expression] = matchInstruction(ts) {
    case parser: ExpressionParser => parser.parseExpression(ts)
    case parser: FunctionCallParser => parser.parseFunctionCall(ts)
  }

  def getExpressionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Expression] = matchInstruction(ts) {
    case parser: ExpressionChainParser => parser.parseExpressionChain(ts, host)
  }

  def getExpressionToConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Condition] = matchInstruction(ts) {
    case parser: ExpressionToConditionPostParser => parser.parseConditionChain(ts, host)
  }

  def getInstructionChain(ts: TokenStream, host: Instruction)(implicit compiler: SQLCompiler): Option[Instruction] = matchInstruction(ts) {
    case parser: InstructionPostfixParser => parser.parseInstructionChain(ts, host)
  }

  def getModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Modifiable] = matchInstruction(ts) {
    case parser: ModifiableParser => Option(parser.parseModifiable(ts))
  }

  def getQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Queryable] = matchInstruction(ts) {
    case parser: QueryableParser => Option(parser.parseQueryable(ts))
  }

  def getQueryableChain(ts: TokenStream, host: Queryable)(implicit compiler: SQLCompiler): Option[Queryable] = matchInstruction(ts) {
    case parser: QueryableChainParser => Option(parser.parseQueryableChain(ts, host))
  }

  def getFunctionCall(ts: TokenStream)(implicit compiler: SQLCompiler): Option[FunctionCall] = matchInstruction(ts) {
    case parser: FunctionCallParser => parser.parseFunctionCall(ts)
  }

  def getStatement(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Instruction] = matchInstruction(ts) {
    case parser: InvokableParser => Option(parser.parseInvokable(ts))
  }

  def getKeywords: List[String] = antiFunctionParsers.flatMap(_.help.collect { case c if c.name.forall(_.isLetter) => c.name })

  def helpDocs: List[HelpDoc] = (MacroLanguageParser :: languageParsers ::: dataTypeParsers).flatMap(_.help).sortBy(_.name)

  def isFunctionCall(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    !ts.isBackticks && !ts.isQuoted && functionCallParsers.exists(_.understands(ts))
  }

  def isInstruction(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    !ts.isBackticks && !ts.isQuoted && (antiFunctionParsers.exists(_.understands(ts)) || MacroLanguageParser.understands(ts))
  }

  private def antiFunctionParsers: List[LanguageParser] = languageParsers.filterNot(_.isInstanceOf[FunctionCallParser])

  private def matchType[A](ts: TokenStream)(f: PartialFunction[LanguageParser, Option[A]])(implicit compiler: SQLCompiler): Option[A] = {
    if (ts.isBackticks || ts.isQuoted) None else {
      for {
        instruction_? <- _dataTypeParsers.collectFirst { case parser: ColumnTypeParser if parser.understands(ts) & f.isDefinedAt(parser) => f(parser) }
        instruction <- instruction_?
      } yield instruction
    }
  }

  private def matchInstruction[A](ts: TokenStream)(f: PartialFunction[LanguageParser, Option[A]])(implicit compiler: SQLCompiler): Option[A] = {
    if (ts.isBackticks || ts.isQuoted) None else {
      for {
        instruction_? <- languageParsers.collectFirst { case parser if parser.understands(ts) & f.isDefinedAt(parser) => f(parser) }
        instruction <- instruction_?
      } yield instruction
    }
  }

}

/**
 * Compiler Context
 */
object QweryUniverse {
  private val _classLoader = new DynamicClassLoader(classOf[QweryUniverse].getClassLoader)
  val _dataTypeParsers: List[DataTypeParser] = List(
    // order is significant
    BitArrayType, BlobType, BooleanType, CharType, ClobType, DateTimeType, EnumType, Int8Type, Int16Type, Int32Type,
    Int64Type, Float32Type, Float64Type, NumericType, IntervalType, JsonType, MatrixType, PointerType, RowIDRangeType,
    RowNumberType, SQLXMLType, TableType, UserDefinedDataType, UUIDType, StringType, VarBinaryType, VarCharType, AnyType
  )
  private val _languageParsers: List[LanguageParser] = List[LanguageParser](
    After, AllFields, AlterTable, Amp, AmpAmp, AND, AnonymousFunction, ApplyTo, ArrayExpression, As, Assert, Async, Avg,
    Bar, BarBar, Between, Betwixt, BooleanType,
    Case, CodeBlock, ClassOf, CodecOf, ColonColon, ColonColonColon, Contains, Count, CountUnique, CreateExternalTable,
    CreateFunction, CreateIndex, CreateMacro, CreateProcedure, CreateTable, CreateType, CreateUniqueIndex, CreateView,
    DeclareClass, DeclarePackage, DeclareTable, DeclareView, DefineFunction, Delete, Describe, DefineImplicit, Destroy,
    Div, DoWhile, Drop,
    Each, ElementAt, EOL, EQ, Every, Exists, Explode, Expose,
    Feature, From,
    Graph, GreaterGreater, GreaterGreaterGreater, GroupBy, GT, GTE,
    HashTag, Having, Help, Http,
    IF, Iff, Import, ImportImplicitClass, Include, Infix, IN, InsertInto, InterfacesOf, Intersect, Into, InvokeVirtualMethod,
    Is, IsCodecOf, IsDefined, IsJavaMember, IsNotNull, Isnt, IsNull,
    Join,
    LessLess, LessLessLess, Let, Like, Limit, LT, LTE,
    Macro, Max, MembersOf, Min, Minus, MinusMinus, InstructionChain, Monadic,
    Namespace, NEG, NEQ, New, NodeAPI, NodeConsole, NodeExec, NodeScan, NodeStart, NodeStop, NodeWWW, Not,
    NotImplemented, NS, Null,
    ObjectOf, Once, OR, OrderBy,
    Pagination, Percent, PercentPercent, Plus, PlusPlus, ProcedureCall,
    Require, Reset, Return, RowsOfValues,
    ScaleTo, Scenario, Select, SetVariable, SetVariableExpression, SpreadOperator, Subtraction, Sum, SuperClassesOf, Synchronized,
    Table, TableLike, TableLiteral, TableZoo, This, ThrowException, Times, TimesTimes, Trace, TransferFrom, TransferTo,
    TryCatch, Truncate, TupleLiteral, TypeOf,
    UnDelete, Union, Unique, UnNest, Up, Update, UpsertInto,
    ValVar, VariableRef, Verify,
    WhileDo, WhenEver, Where, WhereIn, With,
    ZipWith
  )

  RuntimePlatform.init()

}