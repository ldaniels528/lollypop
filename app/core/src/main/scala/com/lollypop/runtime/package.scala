package com.lollypop

import _root_.lollypop.lang.Null
import com.lollypop.database.QueryResponse
import com.lollypop.language._
import com.lollypop.language.implicits._
import com.lollypop.language.models._
import com.lollypop.repl.symbols.REPLSymbol
import com.lollypop.runtime.conversions.TableConversion.convertTupleToTable
import com.lollypop.runtime.conversions.TransferTools.{LimitedInputStream, LimitedReader}
import com.lollypop.runtime.conversions.{DictionaryConversion, InputStreamConversion, OutputStreamConversion}
import com.lollypop.runtime.datatypes.Inferences.fromClass
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.RowCollection.dieNotInnerTableColumn
import com.lollypop.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.lollypop.runtime.devices._
import com.lollypop.runtime.instructions.ReferenceInstruction
import com.lollypop.runtime.instructions.conditions._
import com.lollypop.runtime.instructions.expressions.ArrayLiteral
import com.lollypop.runtime.instructions.functions.Iff
import com.lollypop.runtime.instructions.invocables.{IF, Return, Switch}
import com.lollypop.runtime.instructions.queryables._
import com.lollypop.runtime.plastics.RuntimeClass.implicits.{RuntimeClassConstructorSugar, RuntimeClassProduct}
import com.lollypop.util.DateHelper
import com.lollypop.util.StringRenderHelper.StringRenderer
import lollypop.io.IOCost
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import spray.json._

import java.io._
import java.nio.{Buffer, ByteBuffer}
import java.time.ZoneId
import java.util.concurrent.TimeUnit
import java.util.{Base64, Calendar, Date, TimeZone, UUID}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}
import scala.language.{implicitConversions, postfixOps, reflectiveCalls}
import scala.util.Properties

/**
 * Runtime package object
 */
package object runtime extends AppConstants {
  private val logger = LoggerFactory.getLogger(getClass)

  type ROWID = Long

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //      CURRENT WORKING DIRECTORY
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private val _CWD_ = "__current_working_directory__"
  private val _OLD_CWD_ = "__previous_working_directory__"

  def getCWD(implicit scope: Scope): String = getOSPath(_CWD_)

  private def getOldCWD(implicit scope: Scope): String = getOSPath(_OLD_CWD_)

  private def getOSPath(name: String)(implicit scope: Scope): String = {
    scope.resolveAs[String](name) || new File(".").getCanonicalPath
  }

  /**
   * ChDir-Scope Enrichment
   * @param scope the host [[Scope]]
   */
  final implicit class ChDirScope(val scope: Scope) extends AnyVal {
    @inline
    def chdir(cwd: String, owd: String): Scope = scope.withVariable(_CWD_, cwd).withVariable(_OLD_CWD_, owd)
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //      UTILITIES
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns the option of the given value as the desired type
   * @param value the given value
   * @tparam T the desired type
   * @return the option of the given value casted as the desired type
   */
  def safeCast[T](value: Any): Option[T] = value match {
    case null => None
    case v: T => Option(v)
    case x =>
      logger.warn(s"Failed to cast '$value' (${Option(x).map(_.getClass.getName).orNull})")
      Option.empty[T]
  }

  /**
   * Executes the block capturing the execution time
   * @param block the block to execute
   * @tparam T the result type
   * @return a tuple containing the result and execution time in milliseconds
   */
  def time[T](block: => T): (T, Double) = {
    val startTime = System.nanoTime()
    val result = block
    val elapsedTime = (System.nanoTime() - startTime).toDouble / 1e+6
    (result, elapsedTime)
  }

  private def makeString(reader: Reader, delim: String): String = {
    val sb = new StringBuilder()
    val bufReader = new BufferedReader(reader)
    var line: String = null
    do {
      line = bufReader.readLine()
      if (line != null) sb.append(line).append(delim)
    } while (line != null)
    sb.toString().trim
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //      IMPLICIT CLASSES
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Automatically closes a resource after the completion of a code block
   */
  final implicit class AutoClose[T <: AutoCloseable](val resource: T) extends AnyVal {
    @inline
    def use[S](block: T => S): S = try block(resource) finally {
      try resource.close() catch {
        case e: Exception =>
          logger.error(s"Failure occurred while closing resource $resource", e)
      }
    }
  }

  /**
   * Conversion: Boolean-to-Int
   * @param bool the [[Boolean boolean]] value
   */
  final implicit class Boolean2Int(val bool: Boolean) extends AnyVal {
    @inline def toInt: Int = if (bool) 1 else 0
  }

  /**
   * Calendar Utilities
   * @param host the host [[Date date]]
   */
  final implicit class CalendarUtilities(val host: Date) extends AnyVal {

    @inline def dayOfMonth: Int = toCalendar.get(Calendar.DAY_OF_MONTH)

    @inline def dayOfWeek: Int = toCalendar.get(Calendar.DAY_OF_WEEK)

    @inline def hour: Int = toCalendar.get(Calendar.HOUR_OF_DAY)

    @inline def millisecond: Int = toCalendar.get(Calendar.MILLISECOND)

    @inline def minute: Int = toCalendar.get(Calendar.MINUTE)

    @inline def month: Int = toCalendar.get(Calendar.MONTH) + 1

    @inline def second: Int = toCalendar.get(Calendar.SECOND)

    @inline
    def split: (java.time.LocalDate, java.time.LocalTime) = {
      val date = host.toInstant.atZone(ZoneId.of("GMT")).toLocalDate
      val time = host.toInstant.atZone(ZoneId.of("GMT")).toLocalTime
      (date, time)
    }

    @inline def toTimestamp = new java.sql.Timestamp(host.getTime)

    @inline def year: Int = toCalendar.get(Calendar.YEAR)

    private def toCalendar: Calendar = {
      val cal = Calendar.getInstance()
      cal.setTimeZone(TimeZone.getTimeZone("GMT"))
      cal.setTime(host)
      cal
    }

  }

  /**
   * Column TableType Extension
   * @param column the [[TableColumn]]
   */
  final implicit class ColumnTableType(val column: TableColumn) extends AnyVal {
    @inline
    def getTableType: TableType = column.`type` match {
      case tableType: TableType => tableType
      case other => dieIllegalType(other)
    }
  }

  /**
   * Condition Higher-Order Functions
   * @param condition the host [[Condition condition]]
   */
  final implicit class ConditionHigherOrderFunctions(val condition: Condition) extends AnyVal {

    def collect[A](f: PartialFunction[Condition, A]): List[A] = {
      @inline
      def resolve(condition: Condition): List[A] = {
        condition match {
          case AND(a, b) => resolve(a) ::: resolve(b)
          case Not(c) => resolve(c)
          case OR(a, b) => resolve(a) ::: resolve(b)
          case c if f.isDefinedAt(c) => List(f(c))
          case _ => Nil
        }
      }

      resolve(condition)
    }

    def rewrite(f: PartialFunction[Condition, Option[Condition]]): Option[Condition] = {
      @inline
      def filter(condition: Condition): Option[Condition] = {
        condition match {
          case AND(a, b) => reduce(a, b, AND.apply)(filter)
          case Not(c) => filter(c).map(Not.apply)
          case OR(a, b) => reduce(a, b, OR.apply)(filter)
          case c if f.isDefinedAt(c) => f(c)
          case c => Some(c)
        }
      }

      filter(condition)
    }

    def select(f: PartialFunction[Condition, Condition]): Option[Condition] = {
      @inline
      def filter(condition: Condition): Option[Condition] = {
        condition match {
          case AND(a, b) => reduce(a, b, AND.apply)(filter)
          case Not(c) => filter(c).map(Not.apply)
          case OR(a, b) => reduce(a, b, OR.apply)(filter)
          case c if f.isDefinedAt(c) => Option(f(c))
          case _ => None
        }
      }

      filter(condition)
    }

    def transform(f: Expression => Expression): Condition = {
      condition match {
        case AND(a, b) => AND(a.transform(f), b.transform(f))
        case Between(expr, a, b) => Between(f(expr), f(a), f(b))
        case Betwixt(expr, a, b) => Betwixt(f(expr), f(a), f(b))
        case EQ(a, b) => EQ(f(a), f(b))
        case GT(a, b) => GT(f(a), f(b))
        case GTE(a, b) => GTE(f(a), f(b))
        case Is(a, b) => Is(f(a), f(b))
        case Isnt(a, b) => Isnt(f(a), f(b))
        case Matches(a, b) => Matches(f(a), f(b))
        case LT(a, b) => LT(f(a), f(b))
        case LTE(a, b) => LTE(f(a), f(b))
        case NEQ(a, b) => NEQ(f(a), f(b))
        case Not(c) => Not(c.transform(f))
        case OR(a, b) => OR(a.transform(f), b.transform(f))
        case WhereIn(expr, c) => WhereIn(f(expr), c.transform(f))
        case cond => cond
      }
    }

    @inline
    private def reduce(a: Condition, b: Condition, f: (Condition, Condition) => Condition)(filter: Condition => Option[Condition]): Option[Condition] = {
      (filter(a), filter(b)) match {
        case (Some(aa), Some(bb)) => Option(f(aa, bb))
        case (v@Some(_), None) => v
        case (None, v@Some(_)) => v
        case (None, None) => None
      }
    }

  }


  final implicit class MapToTableType(val mapping: QMap[_, _]) extends AnyVal {
    @inline
    def toKeyValueCollection: RowCollection = {
      implicit val out: RowCollection = createQueryResultTable(columns = Seq(
        TableColumn(name = "key", `type` = StringType),
        TableColumn(name = "value", `type` = AnyType)), fixedRowCount = mapping.size)
      mapping.foreach { case (key, value) => out.insert(Map("key" -> key, "value" -> value).toRow) }
      out
    }
  }

  final implicit class MapToRow(val mappings: QMap[String, Any]) extends AnyVal {

    /**
     * Converts the key-values into a row
     * @param collection the implicit [[RecordMetadataIO]]
     * @return the equivalent [[Row]]
     */
    @inline
    def toRow(implicit collection: RecordMetadataIO): Row = toRow(rowID = rowID || collection.getLength)

    /**
     * Converts the key-values into a row
     * @param rowID     the unique row ID
     * @param structure the implicit [[RecordStructure]]
     * @return the equivalent [[Row]]
     */
    @inline
    def toRow(rowID: ROWID)(implicit structure: RecordStructure): Row = toRow(rowID, structure.columns)

    /**
     * Converts the key-values into a row
     * @param columns the collection of [[TableColumn columns]]
     * @return the equivalent [[Row]]
     */
    @inline def toRow(columns: Seq[TableColumn]): Row = toRow(rowID = 0L, columns)

    /**
     * Converts the key-values into a row
     * @param rowID   the unique row ID
     * @param columns the collection of [[TableColumn columns]]
     * @return the equivalent [[Row]]
     */
    @inline
    def toRow(rowID: ROWID, columns: Seq[TableColumn]): Row = {
      Row(id = rowID, metadata = RowMetadata(), columns = columns, fields = columns map { column =>
        Field(name = column.name, metadata = FieldMetadata(column), value = mappings.get(column.name).flatMap(Option(_)) ??
          column.defaultValue.map(_.evaluate()._3).flatMap(Option(_)))
      })
    }

    @inline
    def rowID: Option[ROWID] = mappings.collectFirst { case (name, id: ROWID) if name == ROWID_NAME => id }

  }

  final implicit class RowsToRowCollection(val rows: Seq[Row]) extends AnyVal {
    def toRowCollection: RowCollection = {
      rows.headOption map { firstRow =>
        val out = createQueryResultTable(firstRow.columns, fixedRowCount = rows.length)
        out.insert(firstRow)
        rows.tail.foreach(out.insert)
        out
      } getOrElse dieNoResultSet()
    }
  }

  /**
   * Rich DatabaseObjectRef
   * @param ref the [[DatabaseObjectRef]]
   */
  final implicit class RichDatabaseObjectRef(val ref: DatabaseObjectRef) extends AnyVal {

    /**
     * Provides a function to process an inner-table
     * @param action the call-back function, which provides a tuple containing the [[RowCollection outer-table]]
     *               and the [[TableColumn column]] (within the outer-table) that contains the inner-table.
     * @param scope  the implicit [[Scope scope]]
     * @tparam A the generic return type
     * @return the return value
     */
    def inside[A](action: (RowCollection, TableColumn) => A)(implicit scope: Scope): A = {
      ref match {
        // is it an inner table reference?
        case DatabaseObjectRef.SubTable(outerTableRef, innerTableColumnName) =>
          // get the outer-table, inner-table and outer-table column containing the inner-table
          val outerTable = scope.getRowCollection(outerTableRef)
          val innerTableColumn = outerTable.getColumnByName(innerTableColumnName) match {
            case column if column.`type`.isTable => column
            case column => dieNotInnerTableColumn(column)
          }
          // perform the action
          action(outerTable, innerTableColumn)
        case other => other.dieNotInnerTable()
      }
    }
  }


  final implicit class ByteArrayToBase64(val bytes: Array[Byte]) extends AnyVal {
    @inline def toBase64: String = Base64.getEncoder.encodeToString(bytes)
  }

  final implicit class ByteArrayFromBase64(val string: String) extends AnyVal {
    @inline def fromBase64: Array[Byte] = Base64.getDecoder.decode(string.getBytes())
  }

  final implicit class RichInputStream(val in: InputStream) extends AnyVal {

    @inline def limitTo(length: Long) = new LimitedInputStream(in, length)

    @inline def mkString(delim: String = "\n"): String = makeString(reader = new InputStreamReader(in), delim)

    @inline
    def toBytes: Array[Byte] = {
      val out = new ByteArrayOutputStream()
      IOUtils.copyLarge(in, out)
      out.toByteArray
    }

    @inline def toBase64: String = Base64.getEncoder.encodeToString(toBytes)

  }

  final implicit class RichReader(val reader: Reader) extends AnyVal {

    @inline def limitTo(length: Long) = new LimitedReader(reader, length)

    @inline def mkString(delim: String = "\n"): String = makeString(reader, delim)

    @inline def toBase64: String = Base64.getEncoder.encodeToString(mkString().getBytes())

  }

  /**
   * Rich Condition
   * @param cond0 the [[Condition math condition]]
   */
  final implicit class RichCondition(val cond0: Condition) extends AnyVal {
    @inline
    def negate: Condition = {
      cond0 match {
        case AND(a, b) => OR(a.negate, b.negate)
        case inequality: Inequality => inequality.invert
        case Isnt(a, b) => Is(a, b)
        case Is(a, b) => Isnt(a, b)
        case Not(condition) => condition
        case OR(a, b) => AND(a.negate, b.negate)
        case condition => Not(condition)
      }
    }
  }

  /**
   * Data Type Buffer
   * @param buf the given [[Buffer]]
   */
  final implicit class DataTypeBuffer[A <: Buffer](val buf: A) extends AnyVal {
    @inline
    def flipMe(): buf.type = {
      buf.flip()
      buf
    }
  }

  /**
   * Data Type Byte Buffer
   * @param buf the given [[ByteBuffer byte buffer]]
   */
  final implicit class DataTypeByteBuffer(val buf: ByteBuffer) extends AnyVal {

    @inline def getBoolean: Boolean = buf.get ~> { n => n != 0 }

    @inline def putBoolean(state: Boolean): ByteBuffer = buf.put((if (state) 1 else 0).toByte)

    @inline
    def getBytes: Array[Byte] = {
      val bytes = new Array[Byte](getLength32)
      buf.get(bytes)
      bytes
    }

    @inline def putBytes(bytes: Array[Byte]): ByteBuffer = buf.putInt(bytes.length).put(bytes)

    @inline def getDate: java.util.Date = new java.util.Date(buf.getLong)

    @inline def putDate(date: java.util.Date): ByteBuffer = buf.putLong(date.getTime)

    @inline def getFieldMetadata: FieldMetadata = FieldMetadata.decode(buf.get)

    @inline def putFieldMetadata(fmd: FieldMetadata): ByteBuffer = buf.put(fmd.encode)

    @inline
    def getInterval: FiniteDuration = (getLength64, getLength32) ~> { case (length, unit) => new FiniteDuration(length, TimeUnit.values()(unit)) }

    @inline
    def putInterval(fd: FiniteDuration): ByteBuffer = buf.putLong(fd.length).putInt(fd.unit.ordinal())

    @inline def getRowID: ROWID = buf.getLong

    @inline def putRowID(rowID: ROWID): ByteBuffer = buf.putLong(rowID)

    @inline def getRowMetadata: RowMetadata = RowMetadata.decode(buf.get)

    @inline def putRowMetadata(rmd: RowMetadata): ByteBuffer = buf.put(rmd.encode)

    @inline
    def getLength32: Int = {
      val length = buf.getInt
      assert(length >= 0, "Length must be positive")
      length
    }

    @inline
    def getLength64: Long = {
      val length = buf.getLong
      assert(length >= 0, "Length must be positive")
      length
    }

    @inline
    def getText: String = {
      val bytes = new Array[Byte](getLength32)
      buf.get(bytes)
      new String(bytes)
    }

    @inline
    def getUUID: UUID = new UUID(buf.getLong, buf.getLong)

    @inline
    def putUUID(uuid: UUID): ByteBuffer = buf.putLong(uuid.getMostSignificantBits).putLong(uuid.getLeastSignificantBits)

  }

  /**
   * Date Math Utilities
   * @param host the host [[Date date]]
   */
  final implicit class DateMathematics(val host: Date) extends AnyVal {

    @inline def -(date: Date): FiniteDuration = (host.getTime - date.getTime).millis

    @inline def +(interval: FiniteDuration): Date = DateHelper.from(host.getTime + interval.toMillis)

    @inline def -(interval: FiniteDuration): Date = DateHelper.from(host.getTime - interval.toMillis)

  }

  /**
   * Database Object Reference Detection
   * @param instruction the host [[Instruction instruction]]
   */
  final implicit class DatabaseObjectRefDetection(val instruction: Instruction) extends AnyVal {
    @inline
    def extractReferences: List[DatabaseObjectRef] = {

      def recurse(op: Instruction): List[DatabaseObjectRef] = op match {
        case BinaryOperation(a, b) => recurse(a) ::: recurse(b)
        case BinaryQueryable(a, b) => recurse(a) ::: recurse(b)
        case CodeBlock(ops) => ops.flatMap(recurse)
        case d: DataObject => List(d.ns)
        case d: DatabaseObjectRef => List(d)
        case Describe(source) => recurse(source)
        case From(source) => recurse(source)
        case HashTag(source, _) => recurse(source)
        case IF(_, a, b_?) => recurse(a) ::: b_?.toList.flatMap(recurse)
        case Iff(_, a, b) => recurse(a) ::: recurse(b)
        case Inequality(a, b, _) => recurse(a) ::: recurse(b)
        case Return(value) => value.toList.flatMap(recurse)
        case s: Select => s.from.toList.flatMap(recurse)
        case Switch(_, cases) => cases.flatMap { case Switch.Case(_, a) => recurse(a) }
        case UnaryOperation(a) => recurse(a)
        case r: ReferenceInstruction => List(r.ref)
        case _ => Nil
      }

      recurse(instruction).distinct
    }
  }

  /**
   * Rich Inequality
   * @param inequality the [[Inequality math inequality]]
   */
  final implicit class RichInequality(val inequality: Inequality) extends AnyVal {
    @inline
    def invert: Inequality = inequality match {
      case EQ(a, b) => NEQ(a, b)
      case NEQ(a, b) => EQ(a, b)
      case Is(a, b) => Isnt(a, b)
      case Isnt(a, b) => Is(a, b)
      case LT(a, b) => GTE(a, b)
      case GT(a, b) => LTE(a, b)
      case LTE(a, b) => GT(a, b)
      case GTE(a, b) => LT(a, b)
      case x => die(s"Failed to negate inequality '$x'")
    }
  }

  /**
   * Inline Variable Replacement
   * @param template the string containing the variables to replace (e.g. "iostat $n $m")
   */
  final implicit class InlineVariableReplacement(val template: String) extends AnyVal {

    def replaceVariables()(implicit scope: Scope): String = {
      @tailrec
      def recurse(sb: StringBuilder, previous: Int): String = {
        val isOk = (c: Char) => c.isLetterOrDigit || c == '_'
        sb.indexOf("$", previous) match {
          case -1 => sb.toString()
          case start =>
            var end = start + 1
            while (end < sb.length() && isOk(sb.charAt(end))) end += 1
            val name = sb.substring(start + 1, end)
            val value = scope.resolve(name).map(v => sb.replace(start, end, String.valueOf(v))) || sb
            recurse(value, end)
        }
      }

      recurse(new StringBuilder(template), previous = 0)
    }

  }

  /**
   * Rich Expression
   * @param instruction the host [[Instruction instruction]]
   */
  final implicit class InstructionAlias(val instruction: Instruction) extends AnyVal {
    @inline
    def getAlias: Option[String] = instruction match {
      case expr@NamedExpression(name) => expr.alias ?? Some(name)
      case expr: Expression => expr.alias
      case _ => None
    }
  }

  /**
   * JSON String Conversion
   * @param jsonString the JSON string
   */
  final implicit class JSONStringConversion(val jsonString: String) extends AnyVal {
    @inline def fromJSON[T](implicit reader: JsonReader[T]): T = jsonString.parseJson.convertTo[T]

    @inline def parseJSON: JsValue = jsonString.parseJson
  }

  /**
   * JSON Product Conversion
   * @param value the value to convert
   */
  final implicit class JSONProductConversion[T](val value: T) extends AnyVal {

    @inline
    def toJsValue: JsValue = value.normalize.asInstanceOf[AnyRef] match {
      case Some(value) => value.toJsValue
      case None | null => JsNull
      case a: Array[_] => JsArray(a.map(_.toJsValue): _*)
      case a: Seq[_] => JsArray(a.map(_.toJsValue): _*)
      case b: RowCollection => JsArray(b.toList.map(_.toMap).map(_.toJsValue): _*)
      case b: java.lang.Boolean => JsBoolean(b)
      case c: Class[_] => JsString(c.getName)
      case d: Date => JsString(DateHelper.format(d))
      case j: JsValue => j
      case r: Row => JsObject(r.toMap.map { case (k, v) => k -> v.toJsValue })
      case m: QMap[_, _] => JsObject(m.toMap.map { case (k, v) => k.toString -> v.toJsValue })
      case n: Number => if (n.longValue() == n.doubleValue()) JsNumber(n.longValue()) else JsNumber(n.doubleValue())
      case p: Product =>
        val tuples = (p.productElementNames zip p.productIterator.map(_.toJsValue)).toList
        JsObject(Map(tuples ::: ("_class" -> JsString(p.getClass.getName)) :: Nil: _*))
      case s: String => JsString(s)
      case s: mutable.StringBuilder => JsString(s.toString)
      case s: StringBuffer => JsString(s.toString)
      case u: UUID => JsString(u.toString)
      case _ =>
        // otherwise, just assume it's a POJO
        JsObject(Map((for {
          m <- value.getClass.getMethods if m.getParameterTypes.isEmpty && m.getName.startsWith("get")
          name = m.getName.drop(3) match {
            case "Class" => "_class"
            case other => other.head.toLower + other.tail
          }
          jsValue = m.invoke(value).toJsValue
        } yield name -> jsValue): _*))
    }

    @inline def toJSON(implicit writer: JsonWriter[T]): String = value.toJson.compactPrint

    @inline def toJSONPretty(implicit writer: JsonWriter[T]): String = value.toJson.prettyPrint

  }

  /**
   * JsValue Conversion (Spray)
   * @param jsValue the [[JsValue JSON value]]
   */
  final implicit class JSONValueConversion(val jsValue: JsValue) extends AnyVal {
    @inline
    def unwrapJSON: Any = jsValue match {
      case js: JsArray => js.elements.map(_.unwrapJSON)
      case JsBoolean(value) => value
      case JsNull => null
      case JsNumber(value) => value
      case js: JsObject => js.fields map { case (name, jsValue) => name -> jsValue.unwrapJSON }
      case JsString(value) => value
      case x => dieUnsupportedType(x)
    }

    @inline
    def toExpression: Expression = jsValue match {
      case js: JsArray => ArrayLiteral(value = js.elements.map(_.toExpression).toList)
      case JsBoolean(value) => value
      case JsNull => Null()
      case JsNumber(value) => value.toDouble
      case js: JsObject => js.fields map { case (name, jsValue) => name -> jsValue.unwrapJSON.v }
      case JsString(value) => value
      case x => dieUnsupportedType(x)
    }
  }

  final implicit class JSONAnyToSprayConversion(val value: Any) extends AnyVal {
    @inline
    def toSprayJs: JsValue = {
      import com.lollypop.runtime.ModelsJsonProtocol._

      def recurse(value: Any): JsValue = value match {
        case None | null => JsNull
        case Some(v) => recurse(v)
        case v: Array[_] => JsArray(v.map(recurse): _*)
        case v: Seq[_] => JsArray(v.map(recurse): _*)
        case m: QMap[String, _] => JsObject(fields = m.toMap.map { case (k, v) => k -> recurse(v) })
        case v: BigDecimal => v.toJson
        case v: BigInt => v.toJson
        case v: RowCollection => v.toMapGraph.toJson
        case v: Boolean => v.toJson
        case v: Byte => v.toJson
        case v: Date => DateHelper.format(v).toJson
        case v: Class[_] => s"""classOf("${v.getName}")""".toJson
        case v: Double => v.toJson
        case v: Float => v.toJson
        case v: Int => v.toJson
        case v: Long => v.toJson
        case n: Number => JsNumber(n.doubleValue())
        case v: Short => v.toJson
        case v: String => v.toJson
        case v: Template => v.toString.toJson
        case v => v.render.toJson
        case v => dieUnsupportedType(v)
      }

      recurse(value.normalize)
    }
  }

  final implicit class MagicImplicits[A](val value: A) extends AnyVal {
    @inline def ~>[B](f: A => B): B = f(value)
  }

  final implicit class MagicBoolImplicits(val value: Boolean) extends AnyVal {
    @inline def ==>[A](result: A): Option[A] = if (value) Option(result) else None
  }

  final implicit class NormalizeAny(val value: Any) extends AnyVal {

    @inline
    def normalize: Any = value match {
      case j: JsValue => j.unwrapJSON
      case x => x.normalizeJava
    }

    @inline
    def normalizeArrays: Any = value match {
      case a: Array[_] => a.map(_.normalizeArrays).toSeq
      case x => x.normalize
    }

    @inline
    def normalizeJava: Any = value match {
      case j: java.util.Collection[_] => j.asScala
      case j: java.util.Map[_, _] => j.asScala
      case x => x
    }
  }

  /**
   * Options Comparator
   * @param optionA the host field or value
   */
  final implicit class OptionsComparator(val optionA: Option[Any]) extends AnyVal {
    // TODO convert to Any >= Any

    def >[A <: Comparable[A]](optionB: Option[Any]): Boolean = boilerplate(optionB) {
      case (a: Date, b: Date) => a.compareTo(b) > 0
      case (a: Date, b: Number) => a.getTime > b.doubleValue()
      case (a: Number, b: Date) => a.doubleValue() > b.getTime
      case (a: Number, b: Number) => a.doubleValue() > b.doubleValue()
      case (a: String, b: String) => a.compareTo(b) > 0
      case (a: UUID, b: UUID) => a.compareTo(b) > 0
      case (a, b) => fail(a, b, operator = ">")
    }

    def >=[A <: Comparable[A]](optionB: Option[Any]): Boolean = boilerplate(optionB) {
      case (a: Date, b: Date) => a.compareTo(b) >= 0
      case (a: Date, b: Number) => a.getTime >= b.doubleValue()
      case (a: Number, b: Date) => a.doubleValue() >= b.getTime
      case (a: Number, b: Number) => a.doubleValue() >= b.doubleValue()
      case (a: String, b: String) => a.compareTo(b) >= 0
      case (a: UUID, b: UUID) => a.compareTo(b) >= 0
      case (a, b) => fail(a, b, operator = ">=")
    }

    def <(optionB: Option[Any]): Boolean = boilerplate(optionB) {
      case (a: Char, b: Char) => a.compareTo(b) < 0
      case (a: Date, b: Date) => a.compareTo(b) < 0
      case (a: Date, b: Number) => a.getTime < b.doubleValue()
      case (a: Number, b: Date) => a.doubleValue() < b.getTime
      case (a: Number, b: Number) => a.doubleValue() < b.doubleValue()
      case (a: String, b: String) => a.compareTo(b) < 0
      case (a: UUID, b: UUID) => a.compareTo(b) < 0
      case (a, b) => fail(a, b, operator = "<")
    }

    def <=[A <: Comparable[A]](optionB: Option[Any]): Boolean = boilerplate(optionB) {
      case (a: Date, b: Date) => a.compareTo(b) <= 0
      case (a: Date, b: Number) => a.getTime <= b.doubleValue()
      case (a: Number, b: Date) => a.doubleValue() <= b.getTime
      case (a: Number, b: Number) => a.doubleValue() <= b.doubleValue()
      case (a: String, b: String) => a.compareTo(b) <= 0
      case (a: UUID, b: UUID) => a.compareTo(b) <= 0
      case (a, b) => fail(a, b, operator = "<=")
    }

    @tailrec
    def unwrapMe: Option[Any] = optionA match {
      case Some(o: Option[_]) => o.unwrapMe
      case x => x
    }

    private def boilerplate(optionB: Option[Any])(f: PartialFunction[(Any, Any), Boolean]): Boolean = {
      (for {
        a <- optionA.unwrapMe
        b <- optionB.unwrapMe
      } yield f(a, b)).contains(true)
    }

    private def fail(a: Any, b: Any, operator: String): Nothing = {
      die(s"Could not compare: '$a' $operator '$b' (${Option(a).map(_.getClass.getName).orNull}, ${Option(b).map(_.getClass.getName).orNull})")
    }

  }

  /**
   * Options Unwrapping
   * @param item the [[Any item]]
   */
  final implicit class OptionsUnwrapping(val item: Any) extends AnyVal {
    @inline def unwrapOptions: Any = {
      @tailrec
      def recurse(value: Any): Any = value match {
        case Some(v) => recurse(v)
        case None => null
        case v => v
      }

      recurse(item)
    }
  }

  final implicit class ProductClassToTableType[T <: Product](val `class`: Class[T]) extends AnyVal {
    @inline
    def toTableType: TableType = {
      val fields = `class`.getFilteredDeclaredFields
      TableType(columns = fields.map { field => TableColumn(field.getName, `type` = fromClass(field.getType)) })
    }
  }

  final implicit class ProductToRowCollection[T <: Product](val product: T) extends AnyVal {
    def toRowCollection: RowCollection = {
      product match {
        case result: QueryResponse =>
          val out = createQueryResultTable(result.columns, fixedRowCount = result.rows.size)
          for {
            values <- result.rows
            row = Map(result.columns zip values map { case (column, value) => column.name -> value }: _*).toRow(out)
          } out.insert(row)
          out
        case row: Row =>
          val out = createQueryResultTable(row.columns, fixedRowCount = 1)
          out.insert(row)
          out
        case mapping: QMap[_, _] => mapping.toKeyValueCollection
        case _ =>
          val fields = product.productElementFields
          val columns = fields.map { field =>
            TableColumn(name = field.getName, `type` = fromClass(field.getType))
          }
          val out = createQueryResultTable(columns, fixedRowCount = 1)
          out.insert(Map(fields.map(_.getName) zip product.productIterator map {
            case (name, value) => name -> value
          }: _*).toRow(out))
          out
      }
    }

    @inline def toTableType: TableType = product.getClass.toTableType
  }

  final implicit class ProductToTableType[A <: Product](val self: A) extends AnyVal {
    @inline
    def asTableType: TableType = {
      TableType(columns = self.productElementFields.map { field =>
        TableColumn(field.getName, `type` = fromClass(field.getType))
      })
    }
  }

  /**
   * Conversion: (Scope, IOCost, A) to (Scope, IOCost, B)
   * @param t the host ([[Scope]], [[IOCost]], `T`) tuple
   */
  final implicit class ScopeIOCostAnyConversion[T](val t: (Scope, IOCost, T)) extends AnyVal {

    @inline
    def map[A](f: T => A): (Scope, IOCost, A) = (t._1, t._2, f(t._3))

    @inline
    def map[A](c: IOCost => IOCost, f: T => A): (Scope, IOCost, A) = (t._1, t._2 ++ c(t._2), f(t._3))

    @inline
    def map[A](s: Scope => Scope, c: IOCost => IOCost, f: T => A): (Scope, IOCost, A) = (s(t._1), c(t._2), f(t._3))

  }

  /**
   * SQL Decompiler Helper
   * @param opCode the [[AnyRef opCode]] to decompile
   */
  final implicit class SQLDecompilerHelper(val opCode: Any) extends AnyVal {
    @inline
    def toSQL: String = opCode match {
      case d: Instruction => d.toSQL
      case x => x.renderAsJson
    }
  }

  /**
   * LollypopVM SQL Execution
   * @param sql the SQL statement or query
   */
  final implicit class LollypopVMSQL(val sql: String) extends AnyVal {

    /**
     * Executes a SQL statement or query
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope scope]], [[IOCost]] and the return value
     */
    @inline
    def executeSQL(scope: Scope): (Scope, IOCost, Any) = {
      LollypopVM.execute(scope, scope.getCompiler.compile(sql))
    }

    /**
     * Executes an SQL query
     * @param scope the [[Scope scope]]
     * @return the potentially updated [[Scope scope]] and the resulting [[RowCollection row collection]]
     */
    @inline
    def searchSQL(scope: Scope): (Scope, IOCost, RowCollection) = {
      scope.getCompiler.compile(sql).search(scope)
    }

  }

  /**
   * LollypopVM Instruction Execution Add-ons
   * @param instruction the [[Instruction instruction]]
   */
  final implicit class LollypopVMAddOns(val instruction: Instruction) extends AnyVal {

    /**
     * Evaluates a pure expression
     * @return a tuple containing the updated [[Scope scope]], [[IOCost]] and the return value
     */
    @inline
    def evaluate(): (Scope, IOCost, Any) = LollypopVM.execute(LollypopVM.rootScope, instruction)

    /**
     * Executes an instruction
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope scope]], [[IOCost]] and the return value
     */
    @inline
    def execute(scope: Scope): (Scope, IOCost, Any) = LollypopVM.execute(scope, instruction)

    /**
     * Evaluates an [[Instruction instruction]]
     * @param scope the [[Scope scope]]
     * @return the potentially updated [[Scope scope]] and the resulting [[RowCollection block device]]
     */
    @inline
    def search(scope: Scope): (Scope, IOCost, RowCollection) = {
      LollypopVM.execute(scope, instruction) match {
        case (aScope, aCost, qr: QueryResponse) => (aScope, aCost, qr.toRowCollection)
        case (aScope, aCost, rc: RowCollection) => (aScope, aCost, rc)
        case (aScope, aCost, rendering: TableRendering) => (aScope, aCost, rendering.toTable(scope))
        case (aScope, aCost, other) => (aScope, aCost, convertTupleToTable(singleColumnResultName, other))
      }
    }

  }

  /**
   * LollypopVMSQL Instruction Sequence Extensions
   * @param instructions the collection of [[Instruction instructions]] to execute
   */
  final implicit class LollypopVMSeqAddOns(val instructions: Seq[Instruction]) extends AnyVal {

    /**
     * Evaluates a collection of instructions
     * @return the tuple consisting of the [[Scope]], [[IOCost]] and the collection of results
     */
    @inline
    def transform(scope0: Scope): (Scope, IOCost, List[Any]) = {
      instructions.foldLeft[(Scope, IOCost, List[Any])]((scope0, IOCost.empty, Nil)) {
        case ((scope, cost, list), op) => LollypopVM.execute(scope, op) ~> { case (s, c, r) => (s, cost ++ c, list ::: List(r)) }
      }
    }

  }

  /**
   * Expressive Type Conversion
   * @param instruction the [[Instruction instruction]]
   */
  final implicit class ExpressiveTypeConversion(val instruction: Instruction) extends AnyVal {

    /**
     * Evaluates an expression casting the result to type `A`
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]]
     *         and the option of a return value.
     */
    @inline
    def pullOpt[A](implicit scope: Scope): (Scope, IOCost, Option[A]) = {
      pull(x => safeCast[A](x))(scope)
    }

    /**
     * Evaluates an expression applying a transformation function to the result.
     * @param f     the `Any` to `A` transformation function
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and the return value.
     */
    @inline
    def pull[A](f: Any => A)(implicit scope: Scope): (Scope, IOCost, A) = {
      instruction.execute(scope) map f
    }

    /**
     * Evaluates an expression converting the result to an array value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and an [[Array]].
     */
    @inline
    def pullArray(implicit scope: Scope): (Scope, IOCost, Array[_]) = {
      instruction.execute(scope) map ArrayType.convert
    }

    /**
     * Evaluates an expression converting the result to an array value.
     * @param f     the `Array[_]` to `Any` transformation function
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and an `A`.
     */
    @inline
    def pullArray[A](f: Array[_] => A)(implicit scope: Scope): (Scope, IOCost, A) = {
      pullArray(scope) map f
    }

    /**
     * Evaluates an expression converting the result to an boolean value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[Boolean]].
     */
    @inline
    def pullBoolean(implicit scope: Scope): (Scope, IOCost, Boolean) = {
      instruction.execute(scope) map BooleanType.convert
    }

    /**
     * Evaluates an expression converting the result to a byte array value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and an [[Array]]
     */
    @inline
    def pullByteArray(implicit scope: Scope): (Scope, IOCost, Array[Byte]) = {
      pull[Array[Byte]](x => VarBinaryType.convert(x))(scope)
    }

    /**
     * Evaluates an expression converting the result to a byte array value.
     * @param f     the `Array[Byte]` to `A` transformation function
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and an `A`.
     */
    @inline
    def pullByteArray[A](f: Array[Byte] => A)(implicit scope: Scope): (Scope, IOCost, A) = {
      pullByteArray(scope) map f
    }

    /**
     * Evaluates an expression converting the result to a date value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[java.util.Date]].
     */
    @inline
    def pullDate(implicit scope: Scope): (Scope, IOCost, java.util.Date) = {
      instruction.execute(scope) map DateTimeType.convert
    }

    /**
     * Evaluates an expression converting the result to a date value then
     * applies the transformation function `f` to the date.
     * @param f     the [[java.util.Date]] to `A` transformation function
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and an `A`.
     */
    @inline
    def pullDate[A](f: java.util.Date => A)(implicit scope: Scope): (Scope, IOCost, A) = {
      pullDate(scope) map f
    }

    /**
     * Evaluates an expression converting the result to a date value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[java.util.Date]].
     */
    @inline
    def pullDictionary(implicit scope: Scope): (Scope, IOCost, QMap[String, Any]) = {
      instruction.execute(scope) map DictionaryConversion.convert
    }

    /**
     * Evaluates an expression converting the result to a double value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[Double]].
     */
    @inline
    def pullDouble(implicit scope: Scope): (Scope, IOCost, Double) = {
      instruction.execute(scope) map Float64Type.convert
    }

    /**
     * Evaluates an expression converting the result to a duration value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[FiniteDuration]].
     */
    @inline
    def pullDuration(implicit scope: Scope): (Scope, IOCost, FiniteDuration) = {
      instruction.execute(scope) map DurationType.convert
    }

    /**
     * Evaluates an expression converting the result to a [[File]] value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[File]].
     */
    def pullFile(implicit scope: Scope): (Scope, IOCost, File) = {
      def decode(sc: Scope, opCode: Instruction): (Scope, IOCost, List[String]) = opCode match {
        // e.g. cd games/emulators/atari/jaguar
        case op@BinaryOperation(a, b) =>
          val (sa, ca, va) = decode(sc, a)
          decode(sa, b) map (ca ++ _, va ::: op.operator :: _)
        // e.g. cd Downloads
        case IdentifierRef(name) if !scope.isDefined(name) => (scope, IOCost.empty, List(name))
        // e.g. ls ^/Documents; cd _; cd ..; www https://0.0.0.0/api?symbol=ABC
        case r: REPLSymbol => (scope, IOCost.empty, List(r.symbol))
        // anything else ...
        case z => z.pullString map (x => List(x))
      }

      // expand special characters ('^', '~', '_', '.', '..')
      decode(scope, instruction) map (_.mkString) map {
        case "_" => getOldCWD
        case "." => getCWD
        case ".." => new File(getCWD).getParent
        case s if s.startsWith("^") => s.drop(1)
        case s if s.startsWith("~") => Properties.userHome + s.drop(1)
        case s => s
      } map (new File(_))
    }

    /**
     * Evaluates an expression converting the result to a [[File]] value then
     * applies the transformation function `f` resulting in an `A` value.
     * @param f     the [[File file]] to `A` transformation function
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[File]].
     */
    @inline
    def pullFile[A](f: File => A)(implicit scope: Scope): (Scope, IOCost, A) = {
      pullFile(scope) map f
    }

    /**
     * Evaluates an expression converting the result to a float value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[Float]].
     */
    @inline
    def pullFloat(implicit scope: Scope): (Scope, IOCost, Float) = {
      instruction.execute(scope) map Float32Type.convert
    }

    /**
     * Evaluates and converts the [[Expression]] into an [[InputStream]]
     * @return a tuple of the [[Scope]], [[IOCost]] and [[InputStream]]
     */
    @inline
    def pullInputStream(implicit scope: Scope): (Scope, IOCost, InputStream) = {
      instruction.execute(scope) map InputStreamConversion.convert
    }

    /**
     * Evaluates and converts the [[Expression]] into an [[OutputStream]]
     * @return a tuple of the [[Scope]], [[IOCost]] and [[OutputStream]]
     */
    @inline
    def pullOutputStream(implicit scope: Scope): (Scope, IOCost, OutputStream) = {
      instruction.execute(scope) map OutputStreamConversion.convert
    }

    /**
     * Evaluates an expression converting the result to a [[Int]] value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[Int]].
     */
    @inline
    def pullInt(implicit scope: Scope): (Scope, IOCost, Int) = {
      instruction.execute(scope) map Int32Type.convert
    }

    /**
     * Evaluates an expression converting the result to a [[Number]] value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[Number]].
     */
    @inline
    def pullNumber(implicit scope: Scope): (Scope, IOCost, Number) = {
      instruction.execute(scope) map NumericType.convert
    }

    /**
     * Evaluates an expression converting the result to a [[Number]] value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a `A`.
     */
    @inline
    def pullNumber[A](f: Number => A)(implicit scope: Scope): (Scope, IOCost, A) = {
      pullNumber(scope) map f
    }

    /**
     * Evaluates an expression converting the result to a [[String]] value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[String]].
     */
    @inline
    def pullString(implicit scope: Scope): (Scope, IOCost, String) = {
      instruction.execute(scope) map StringType.convert
    }

    /**
     * Evaluates an expression converting the result to a [[String]] value then
     * applies the transformation function `f` resulting in an `A` value.
     * @param f     the [[String]] to `A` transformation function
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[String]].
     */
    @inline
    def pullString[A](f: String => A)(implicit scope: Scope): (Scope, IOCost, A) = {
      pullString(scope) map f
    }

  }

}
