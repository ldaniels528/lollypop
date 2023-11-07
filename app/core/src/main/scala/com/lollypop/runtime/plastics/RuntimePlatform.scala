package com.lollypop.runtime.plastics

import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.language.models._
import com.lollypop.language.{dieExpectedArray, dieIllegalType}
import com.lollypop.runtime.LollypopVM.convertToTable
import com.lollypop.runtime.datatypes.{DateTimeType, Inferences}
import com.lollypop.runtime.devices.RowCollectionZoo.{MapToTableType, ProductToRowCollection}
import com.lollypop.runtime.devices.{QMap, RowCollection}
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.instructions.functions.{AnonymousFunction, NamedFunction}
import com.lollypop.runtime.instructions.queryables.{RuntimeQueryable, TableRendering}
import com.lollypop.runtime.plastics.RuntimeClass.implicits.RuntimeClassConstructorSugar
import com.lollypop.runtime.plastics.RuntimeClass.registerVirtualMethod
import com.lollypop.runtime.plastics.Tuples.seqToArray
import com.lollypop.runtime.{LollypopVM, Scope, safeCast}
import com.lollypop.util.CalendarHelper.CalendarUtilities
import com.lollypop.util.CodecHelper._
import com.lollypop.util.DateHelper
import com.lollypop.util.DateOperations.DateMathematics
import com.lollypop.util.JSONSupport.{JSONProductConversion, JSONStringConversion}
import com.lollypop.util.JVMSupport.NormalizeAny
import com.lollypop.util.StringRenderHelper.StringRenderer
import lollypop.io.IOCost
import org.apache.commons.codec.binary.Hex
import spray.json.JsArray

import java.math.BigInteger
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.{Base64, Date, UUID}
import scala.annotation.tailrec
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.util.Try

/**
 * Runtime Platform
 */
object RuntimePlatform {
  private val ANY_TYPE = Some("Any".ct)
  private val ARRAY_TYPE = Some(classOf[Array[AnyRef]].getName.ct)
  private val BYTE_ARRAY_TYPE = Some(classOf[Array[Byte]].getName.ct)
  private val BOOLEAN_TYPE = Some("Boolean".ct)
  private val CHAR_TYPE = Some("Char".ct)
  private val DATETIME_TYPE = Some("DateTime".ct)
  private val JSON_TYPE = Some("JSON".ct)
  private val INT_TYPE = Some("Int".ct)
  private val INTERVAL_TYPE = Some("Interval".ct)
  private val NUMERIC_TYPE = Some("Numeric".ct)
  private val STRING_TYPE = Some("String".ct)
  private val TABLE_TYPE = Some(classOf[RowCollection].getName.ct)
  private val TUPLE2_TYPE = Some(classOf[Tuple2[_, _]].getName.ct)

  /**
   * Any<T> extensions
   */
  private object Anything {

    def init(): Unit = {
      // Returns true if the field or variable is an Array: x.isArray()
      anyFunction0(name = "isArray", _.getClass.isArray, returnType_? = BOOLEAN_TYPE)

      // Returns true if the field or variable exists within the scope: x.isDateTime()
      anyFunction0(name = "isDateTime", isDateTime, returnType_? = BOOLEAN_TYPE)

      // Returns true if the field or variable is a Function type: x.isFunction()
      anyFunction0(name = "isFunction", _.isInstanceOf[Function], returnType_? = BOOLEAN_TYPE)

      // Returns true if argument is a ISO date (8601) string value, false otherwise: x.isISO8601()
      anyFunction0(name = "isISO8601", s => Try(DateHelper.parse(String.valueOf(s))).isSuccess, returnType_? = BOOLEAN_TYPE)

      // Returns true if argument is a numeric value, false otherwise: x.isNumber()
      anyFunction0(name = "isNumber", _.isInstanceOf[Number], returnType_? = BOOLEAN_TYPE)

      // Returns true if argument is a string value, false otherwise: x.isString()
      anyFunction0(name = "isString", _.isInstanceOf[String], returnType_? = BOOLEAN_TYPE)

      // Returns true if argument is a string value, false otherwise: x.isTable()
      anyFunction0(name = "isTable", _.isInstanceOf[RowCollection], returnType_? = BOOLEAN_TYPE)

      // Returns true if argument is a UUID value, false otherwise: x.isUUID()
      anyFunction0(name = "isUUID", v => v.isInstanceOf[UUID] | Try(UUID.fromString(String.valueOf(v))).isSuccess, returnType_? = BOOLEAN_TYPE)

      // Translate a Java object into a byte array: "Hello World".serialize()
      anyFunction0(name = "serialize", serialize, returnType_? = BYTE_ARRAY_TYPE)

      // Returns the JSON representation of an object: [{Hello:'World'}].toJsonString()
      anyFunction0(name = "toJson", _.toJsValue, returnType_? = JSON_TYPE)

      // Returns the compact JSON String representation of an object: [{Hello:'World'}].toJsonString()
      anyFunction0(name = "toJsonString", _.toJsValue.compactPrint, returnType_? = STRING_TYPE)

      // Returns the pretty JSON String representation of an object: [{Hello:'World'}].toJsonPretty()
      anyFunction0(name = "toJsonPretty", _.toJsValue.prettyPrint, returnType_? = STRING_TYPE)

      // Returns a pretty formatted string: ['H', 'E', 'L', 'L', 'O'].toPrettyString()
      anyFunction0(name = "toPrettyString", _.renderPretty, returnType_? = STRING_TYPE)

      // Returns the table representation of the object: [{a:1,b:0,c:7},{a:3,b:5,c:9}].toTable()
      registerVirtualMethod(VirtualMethod(_ => true, fx = NamedFunction(
        name = "toTable",
        params = Seq("value Any".c),
        code = new ToTable("value".f),
        returnType_? = TABLE_TYPE
      )))
    }

    private def anyFunction0(name: String, f: Any => Any, returnType_? : Option[ColumnType]): Unit = {
      registerVirtualMethod(VirtualMethod(_ => true, fx = NamedFunction(
        name = name,
        params = Seq("value Any".c),
        code = new AnyFunction0(name, "value".f, f),
        returnType_? = returnType_?
      )))
    }

    private def isDateTime(value: Any): Boolean = value match {
      case s: String => Try(DateHelper(s)).isSuccess
      case x => x.isInstanceOf[Date]
    }

  }

  /**
   * Array extensions
   */
  private object Arrays {

    def init(): Unit = {
      // Returns the index of the value if found in the collection: [1, 3, 5, 7, 9].append([2, 4, 8])
      arrayFunction1[Any](name = "append", {
        case (arr0, arr1: Array[_]) => arr0 ++ arr1
        case (_, other) => dieIllegalType(other)
      }, returnType_? = ARRAY_TYPE)

      // Returns the index of the value if found in the collection: [1, 3, 5, 7, 9].contains(5)
      arrayFunction1[Any](name = "contains", (arr, v) => arr.toSeq.contains(v), returnType_? = BOOLEAN_TYPE)

      // Returns a new array; removing duplicate values from the array: [1, 3, 5, 7, 9, 7, 3, 2, 1, null].distinctValues()
      arrayFunction0(name = "distinctValues", _.distinct, returnType_? = ARRAY_TYPE)

      // Returns a new array containing only the values that satisfy the lambda: [1 to 10].filter((n: Int) => (n % 2) == 0)
      arrayFunction1S[LambdaFunction](name = "filter", (arr, fx, scope) => filter(arr, fx)(scope), returnType_? = ARRAY_TYPE)

      // Returns a new array containing only the values that satisfy the lambda: [1 to 10].filterNot((n: Int) => (n % 2) == 0)
      arrayFunction1S[LambdaFunction](name = "filterNot", (arr, fx, scope) => filterNot(arr, fx)(scope), returnType_? = ARRAY_TYPE)

      // Returns the index of the value if found in the collection: ['A' to 'Z'].foldLeft([], (array: Any[], c: Char) => array.push(c))
      arrayFunction2S[Any, LambdaFunction](name = "foldLeft", (arr, init, fx, scope) => foldLeft(arr.map(_.v), init, fx)(scope), returnType_? = ANY_TYPE)

      // Returns the index of the value if found in the collection: ['A' to 'Z'].foreach((c: Char) => out.println(c))
      arrayFunction1S[LambdaFunction](name = "foreach", (arr, fx, scope) => foreach(arr.map(_.v), fx)(scope), returnType_? = ANY_TYPE)

      // Returns the tail of the array: [1 to 5].head()
      arrayFunction0(name = "head", _.head, returnType_? = ANY_TYPE)

      // Returns the tail of the array: [1 to 5].headOption()
      arrayFunction0(name = "headOption", _.headOption, returnType_? = ANY_TYPE)

      // Returns the index of the value if found in the collection: ['A' to 'Z'].indexOf('C')
      arrayFunction1[Any](name = "indexOf", (arr, v) => arr.toSeq.indexOf(v), returnType_? = INT_TYPE)

      // Returns the init of the array: [1 to 5].init()
      arrayFunction0(name = "init", _.init, returnType_? = ARRAY_TYPE)

      // Returns true if the field or variable is an empty Array: x.isEmpty()
      arrayFunction0(name = "isEmpty", _.isEmpty, returnType_? = BOOLEAN_TYPE)

      // Concatenates the elements of the given array using the delimiter: ['A', 'p', 'p', 'l', 'e', '!'].join(',')
      arrayFunction1[Any](name = "join", (arr, v) => arr.mkString(String.valueOf(v)), returnType_? = STRING_TYPE)

      // Returns the length of the given array: [1 to 5].length()
      arrayFunction0(name = "length", _.length, returnType_? = INT_TYPE)

      // Transforms the values within an array: ['A' to 'Z'].map((c: char) => char(c + 127))
      arrayFunction1S[LambdaFunction](name = "map", (arr, fx, scope) => mapValues(arr.map(_.v), fx)(scope), returnType_? = ARRAY_TYPE)

      // Returns the maximum value in the array; null elements are ignored: [1, 3, 5, 7, 9].maxValue()
      arrayFunction0(name = "maxValue", maxValue, returnType_? = ANY_TYPE)

      // Returns the minimum value in the array; null elements are ignored: [1, 3, 5, 7, 9].minValue()
      arrayFunction0(name = "minValue", minValue, returnType_? = ANY_TYPE)

      // Returns true if the field or variable is not an empty Array: x.nonEmpty()
      arrayFunction0(name = "nonEmpty", _.nonEmpty, returnType_? = BOOLEAN_TYPE)

      // Returns the index of the value if found in the collection: ['A' to 'F'].push('Z')
      arrayFunction1[Any](name = "push", (arr, v) => arr.push(v), returnType_? = ARRAY_TYPE)

      // Returns the a new array with elements in reverse order: ['A' to 'Z'].reverse()
      arrayFunction0(name = "reverse", _.reverse, returnType_? = ARRAY_TYPE)

      // Returns the index of the value if found in the collection: ['A' to 'Z'].indexOf('C')
      arrayFunction2[Int, Int](name = "slice", (arr, a, b) => arr.slice(a, b), returnType_? = INT_TYPE)

      // Returns the sorted array. null elements are ignored: [1, 3, 5, 7, 9].sortValues()
      arrayFunction0(name = "sortValues", sortValues, returnType_? = ARRAY_TYPE)

      // Returns the tail of the array: [1 to 5].tail()
      arrayFunction0(name = "tail", _.tail, returnType_? = ARRAY_TYPE)
    }

    ////////////////////////////////////////////////////////////////////////////////
    //  Utility Functions
    ////////////////////////////////////////////////////////////////////////////////

    private def arrayFunction0(name: String, f: Array[_] => Any, returnType_? : Option[ColumnType]): Unit = {
      registerVirtualMethod(VirtualMethod(_.isInstanceOf[Array[_]], fx = NamedFunction(
        name = name,
        params = Seq("array Any".c),
        code = new ArrayFunction0(name, "array".f, f),
        returnType_? = returnType_?
      )))
    }

    private def arrayFunction1[A](name: String, f: (Array[_], A) => Any, returnType_? : Option[ColumnType]): Unit = {
      registerVirtualMethod(VirtualMethod(vm => vm.isInstanceOf[Array[_]], fx = NamedFunction(
        name = name,
        params = Seq("array Any".c, "arg1 Any".c),
        code = new ArrayFunction1(name, "array".f, "arg1".f, f),
        returnType_? = returnType_?
      )))
    }

    private def arrayFunction1S[A](name: String, f: (Array[_], A, Scope) => Any, returnType_? : Option[ColumnType]): Unit = {
      registerVirtualMethod(VirtualMethod(vm => vm.isInstanceOf[Array[_]], fx = NamedFunction(
        name = name,
        params = Seq("array Any".c, "arg1 Any".c),
        code = new ArrayFunction1S(name, "array".f, "arg1".f, f),
        returnType_? = returnType_?
      )))
    }

    private def arrayFunction2[A, B](name: String, f: (Array[_], A, B) => Any, returnType_? : Option[ColumnType]): Unit = {
      registerVirtualMethod(VirtualMethod(vm => vm.isInstanceOf[Array[_]], fx = NamedFunction(
        name = name,
        params = Seq("array Any".c, "arg1 Any".c, "arg2 Any".c),
        code = new ArrayFunction2(name, "array".f, "arg1".f, "arg2".f, f),
        returnType_? = returnType_?
      )))
    }

    private def arrayFunction2S[A, B](name: String, f: (Array[_], A, B, Scope) => Any, returnType_? : Option[ColumnType]): Unit = {
      registerVirtualMethod(VirtualMethod(vm => vm.isInstanceOf[Array[_]], fx = NamedFunction(
        name = name,
        params = Seq("array Any".c, "arg1 Any".c, "arg2 Any".c),
        code = new ArrayFunction2S(name, "array".f, "arg1".f, "arg2".f, f),
        returnType_? = returnType_?
      )))
    }

    private def filter(array: Array[_], fx: LambdaFunction)(implicit scope: Scope): Array[_] = {
      var list: List[_] = Nil
      array.foreach { value =>
        val result = LollypopVM.execute(scope, fx.call(List(value.v)))._3
        if (result == true) list = value :: list
      }
      seqToArray(list.reverse)
    }

    private def filterNot(array: Array[_], fx: LambdaFunction)(implicit scope: Scope): Array[_] = {
      var list: List[_] = Nil
      array.foreach { value =>
        val result = LollypopVM.execute(scope, fx.call(List(value.v)))._3
        if (result == false) list = value :: list
      }
      seqToArray(list.reverse)
    }

    private def foldLeft(array: Array[Expression], initialValue: Any, fx: LambdaFunction)(implicit scope: Scope): Any = {
      array.foldLeft[(Scope, Any)]((scope, initialValue)) { case ((aggScope, aggResult), param) =>
        LollypopVM.execute(aggScope, fx.call(List(aggResult.v, param))) ~> { case (s, c, r) => (s, r) }
      }._2
    }

    private def foreach(args: Seq[Expression], fx: LambdaFunction)(implicit scope: Scope): Unit = {
      @tailrec
      def recurse(scope0: Scope, index: Int = 0): Unit = {
        if (index < args.length) {
          val (scope1, _, _) = LollypopVM.execute(scope0, fx.call(args = List(args(index))))
          recurse(scope1, index = index + 1)
        }
      }

      // reset the anonymous function's scope
      fx match {
        case af: AnonymousFunction => af.updateScope(Scope(scope))
        case _ =>
      }
      recurse(scope)
    }

    private def mapValues(args: Seq[Expression], fx: LambdaFunction)(implicit scope: Scope): Array[_] = {
      val (_, resultB) = args.foldLeft[(Scope, List[Any])](scope -> Nil) {
        case ((aggScope, aggResults), expression) =>
          val (scopeA, _, resultA) = LollypopVM.execute(aggScope, fx.call(List(expression)))
          scopeA -> (resultA :: aggResults)
      }
      resultB.reverse.toArray
    }

    /**
     * Returns the maximum value in the array. null elements are ignored.
     * @param source the array values
     * @return the maximum value or null if the array is empty
     */
    private def maxValue(source: Array[_]): Any = {
      source match {
        case null => null
        case array: Array[_] if array.isEmpty => null
        case array: Array[BigDecimal] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.max }
        case array: Array[BigInteger] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.max }
        case array: Array[Boolean] => array.max
        case array: Array[Byte] => array.max
        case array: Array[Char] => array.max
        case array: Array[Double] => array.max
        case array: Array[FiniteDuration] => array.max
        case array: Array[Float] => array.max
        case array: Array[Int] => array.max
        case array: Array[Long] => array.max
        case array: Array[Short] => array.max
        case array: Array[java.lang.Boolean] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.max }
        case array: Array[java.lang.Byte] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.max }
        case array: Array[java.lang.Character] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.max }
        case array: Array[java.lang.Double] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.max }
        case array: Array[java.lang.Float] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.max }
        case array: Array[java.lang.Integer] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.max }
        case array: Array[java.lang.Long] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.max }
        case array: Array[java.lang.Short] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.max }
        case array: Array[Date] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.max }
        case array: Array[String] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.max }
        case array: Array[UUID] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.max }
        case _ => dieExpectedArray()
      }
    }

    /**
     * Returns the minimum value in the array. null elements are ignored.
     * @param source the array values
     * @return the minimum value or null if the array is empty
     */
    private def minValue(source: Array[_]): Any = {
      source match {
        case array: Array[_] if array.isEmpty => null
        case array: Array[BigDecimal] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.min }
        case array: Array[BigInteger] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.min }
        case array: Array[Boolean] => array.min
        case array: Array[Byte] => array.min
        case array: Array[Char] => array.min
        case array: Array[Double] => array.min
        case array: Array[FiniteDuration] => array.min
        case array: Array[Float] => array.min
        case array: Array[Int] => array.min
        case array: Array[Long] => array.min
        case array: Array[Short] => array.min
        case array: Array[java.lang.Boolean] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.min }
        case array: Array[java.lang.Byte] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.min }
        case array: Array[java.lang.Character] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.min }
        case array: Array[java.lang.Double] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.min }
        case array: Array[java.lang.Float] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.min }
        case array: Array[java.lang.Integer] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.min }
        case array: Array[java.lang.Long] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.min }
        case array: Array[java.lang.Short] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.min }
        case array: Array[Date] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.min }
        case array: Array[String] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.min }
        case array: Array[UUID] => array.filterNot(_ == null) ~> { values => if (values.isEmpty) None else values.min }
        case _ => dieExpectedArray()
      }
    }

    private def sortValues(source: Array[_]): Any = {
      source match {
        case null => null
        case array: Array[BigDecimal] => array.partition(_ != null) ~> { case (values, nulls) => values.sorted ++ nulls }
        case array: Array[BigInteger] => array.partition(_ != null) ~> { case (values, nulls) => values.sorted ++ nulls }
        case array: Array[Boolean] => array.sorted
        case array: Array[Byte] => array.sorted
        case array: Array[Char] => array.sorted
        case array: Array[Double] => array.sorted
        case array: Array[FiniteDuration] => array.sorted
        case array: Array[Float] => array.sorted
        case array: Array[Int] => array.sorted
        case array: Array[Long] => array.sorted
        case array: Array[Short] => array.sorted
        case array: Array[java.lang.Boolean] => array.partition(_ != null) ~> { case (values, nulls) => values.sorted ++ nulls }
        case array: Array[java.lang.Byte] => array.partition(_ != null) ~> { case (values, nulls) => values.sorted ++ nulls }
        case array: Array[java.lang.Character] => array.partition(_ != null) ~> { case (values, nulls) => values.sorted ++ nulls }
        case array: Array[java.lang.Double] => array.partition(_ != null) ~> { case (values, nulls) => values.sorted ++ nulls }
        case array: Array[java.lang.Float] => array.partition(_ != null) ~> { case (values, nulls) => values.sorted ++ nulls }
        case array: Array[java.lang.Integer] => array.partition(_ != null) ~> { case (values, nulls) => values.sorted ++ nulls }
        case array: Array[java.lang.Long] => array.partition(_ != null) ~> { case (values, nulls) => values.sorted ++ nulls }
        case array: Array[java.lang.Short] => array.partition(_ != null) ~> { case (values, nulls) => values.sorted ++ nulls }
        case array: Array[Date] => array.partition(_ != null) ~> { case (values, nulls) => values.sorted ++ nulls }
        case array: Array[String] => array.partition(_ != null) ~> { case (values, nulls) => values.sorted ++ nulls }
        case array: Array[UUID] => array.partition(_ != null) ~> { case (values, nulls) => values.sorted ++ nulls }
        case _ => dieExpectedArray()
      }
    }

    final implicit class EnrichedArray(val array: Array[_]) extends AnyVal {
      def push(value: Any): Array[_] = {
        value match {
          case otherArray: Array[_] =>
            val arrayType0 = array.getClass.getComponentType
            val arrayType1 = otherArray.getClass.getComponentType
            if (!arrayType1.isDescendantOf(arrayType0)) dieIllegalType(s"Type mismatch: ${arrayType0.getName} and ${arrayType1.getName}")
            val newArray = java.lang.reflect.Array.newInstance(arrayType0, array.length + otherArray.length)
            System.arraycopy(array, 0, newArray, 0, array.length)
            System.arraycopy(otherArray, 0, newArray, array.length, otherArray.length)
            newArray.asInstanceOf[Array[_]]
          case _ =>
            val componentType = array.getClass.getComponentType
            val newArray = java.lang.reflect.Array.newInstance(componentType, array.length + 1)
            java.lang.reflect.Array.set(newArray, array.length, value)
            System.arraycopy(array, 0, newArray, 0, array.length)
            newArray.asInstanceOf[Array[_]]
        }
      }
    }

  }

  /**
   * Byte Array extensions
   */
  private object ByteArrays {

    def init(): Unit = {
      // Compresses a byte array using Base64 and returns the compressed data as a byte array: "Hello".getBytes().base64()
      bytesFunction0(name = "base64", Base64.getEncoder.encode, returnType_? = BYTE_ARRAY_TYPE)

      // Decompresses a byte array using Base64 and returns the uncompressed data as a byte array: bytes.unBase64()
      bytesFunction0(name = "unBase64", Base64.getDecoder.decode, returnType_? = BYTE_ARRAY_TYPE)

      // Reconstitutes a Java object from a byte array: "Hello World".serialize().deserialize()
      bytesFunction0(name = "deserialize", deserialize, returnType_? = ANY_TYPE)

      // Compresses a byte array using GZIP and returns the compressed data as a byte array: "Hello".getBytes().gzip()
      bytesFunction0(name = "gzip", compressGZIP, returnType_? = BYTE_ARRAY_TYPE)

      // Decompresses a byte array using GZIP and returns the uncompressed data as a byte array: bytes.gunzip()
      bytesFunction0(name = "gunzip", decompressGZIP, returnType_? = BYTE_ARRAY_TYPE)

      // Return a MD5 digest: "Hello".getBytes().md5()
      bytesFunction0(name = "md5", MessageDigest.getInstance("MD5").digest, returnType_? = BYTE_ARRAY_TYPE)

      // Compresses a byte array using Snappy and returns the compressed data as a byte array: "Hello".getBytes().snappy()
      bytesFunction0(name = "snappy", compressSnappy, returnType_? = BYTE_ARRAY_TYPE)

      // Return the byte array as a hex string: "Hello".getBytes().toHexString()
      bytesFunction0(name = "toHex", _.map(b => f"$b%02x").mkString, returnType_? = STRING_TYPE)

      // Decompresses a byte array using Snappy and returns the uncompressed data as a byte array: bytes.unSnappy()
      bytesFunction0(name = "unSnappy", decompressSnappy, returnType_? = BYTE_ARRAY_TYPE)
    }

    private def bytesFunction0(name: String, f: Array[Byte] => Any, returnType_? : Option[ColumnType]): Unit = {
      registerVirtualMethod(VirtualMethod(_.isInstanceOf[Array[Byte]], fx = NamedFunction(
        name = name,
        params = Seq("value Any".c),
        code = new BytesFunction0(name, "value".f, f),
        returnType_? = returnType_?
      )))
    }

  }

  /**
   * Date extensions
   */
  private object Dates {

    def init(): Unit = {
      // Returns the difference of a Date and a Duration: DateTime('2021-09-02T11:22:33.000Z') - Interval('5 seconds')
      registerVirtualMethod(VirtualMethod(_.isInstanceOf[Date], fx = NamedFunction(
        name = "$minus",
        params = Seq("valueA DateTime".c, "valueB Any".c),
        code = new DateMinus("valueA".f, "valueB".f),
        returnType_? = DATETIME_TYPE
      )))

      // Returns the sum of a Date and a Duration: DateTime('2021-09-02T11:22:33.000Z') + Interval('5 seconds')
      dateFunction1[FiniteDuration](name = "$plus", (a, b) => a + b, returnType_? = DATETIME_TYPE)

      // Returns the day of the month of the a Date: DateTime('2021-09-02T11:22:33.000Z').dayOfWeek()
      dateFunction0(name = "dayOfMonth", _.dayOfMonth, returnType_? = INT_TYPE)

      // Returns the day of the week of the a Date: DateTime('2021-09-02T11:22:33.000Z').dayOfWeek()
      dateFunction0(name = "dayOfWeek", _.dayOfWeek, returnType_? = INT_TYPE)

      // Renders a Date as a string in the specified format: DateTime('2021-09-02T11:22:33.000Z').format('yyyy-MM-dd')
      registerVirtualMethod(VirtualMethod(_.isInstanceOf[Date], fx = NamedFunction(
        name = "format",
        params = Seq("value DateTime".c, "formatString DateTime".c),
        code = new DateFormat("value".f, "formatString".f),
        returnType_? = STRING_TYPE
      )))

      // Returns the hour component of the a Date: DateTime('2021-09-02T11:22:33.000Z').hour()
      dateFunction0(name = "hour", _.hour, returnType_? = INT_TYPE)

      // Returns the millisecond component of the a Date: DateTime('2021-09-02T11:22:33.000Z').millisecond()
      dateFunction0(name = "millisecond", _.millisecond, returnType_? = INT_TYPE)

      // Returns the minute component of the a Date: DateTime('2021-09-02T11:22:33.000Z').minute()
      dateFunction0(name = "minute", _.minute, returnType_? = INT_TYPE)

      // Returns the month component of the a Date: DateTime('2021-09-02T11:22:33.000Z').month()
      dateFunction0(name = "month", _.month, returnType_? = INT_TYPE)

      // Returns the second component of the a Date: DateTime('2021-09-02T11:22:33.000Z').second()
      dateFunction0(name = "second", _.second, returnType_? = INT_TYPE)

      // Returns the date and time component of the a Date as a tuple: DateTime('2021-09-02T11:22:33.000Z').split()
      dateFunction0(name = "split", _.split, returnType_? = TUPLE2_TYPE)

      // Returns the date in HTTP format: DateTime('2021-09-02T11:22:33.000Z').toHttpDate()
      dateFunction0(name = "toHttpDate", DateHelper.toHttpDate, returnType_? = DATETIME_TYPE)

      // Returns the year component of the a Date: DateTime('2021-09-02T11:22:33.000Z').year()
      dateFunction0(name = "year", _.year, returnType_? = INT_TYPE)
    }

    private def dateFunction0(name: String, f: Date => Any, returnType_? : Option[ColumnType]): Unit = {
      registerVirtualMethod(VirtualMethod(classOf[Date], fx = NamedFunction(
        name = name,
        params = Seq("value DateTime".c),
        code = new DateFunction0(name, "value".f, f),
        returnType_? = returnType_?
      )))
    }

    private def dateFunction1[A](name: String, f: (Date, A) => Any, returnType_? : Option[ColumnType]): Unit = {
      registerVirtualMethod(VirtualMethod(`class` = classOf[Date], fx = NamedFunction(
        name = name,
        params = Seq("value DateTime".c, "arg1 Any".c),
        code = new DateFunction1(name, "value".f, "arg1".f, f),
        returnType_? = returnType_?
      )))
    }

  }

  /**
   * Duration extensions
   */
  private object Durations {

    def init(): Unit = {
      // Returns the difference of two Durations: Interval('5 seconds') - Interval('5 seconds')
      durationFunction1[FiniteDuration](name = "$minus", (a, b) => a - b, returnType_? = INTERVAL_TYPE)

      // Returns the sum of two Durations: Interval('5 seconds') + Interval('5 seconds')
      durationFunction1[FiniteDuration](name = "$plus", (a, b) => a + b, returnType_? = INTERVAL_TYPE)
    }

    private def durationFunction1[A](name: String, f: (FiniteDuration, A) => Any, returnType_? : Option[ColumnType]): Unit = {
      registerVirtualMethod(VirtualMethod(`class` = classOf[FiniteDuration], fx = NamedFunction(
        name = name,
        params = Seq("value Interval".c, "arg1 Any".c),
        code = new DurationFunction1(name, "value".f, "arg1".f, f),
        returnType_? = returnType_?
      )))
    }

  }

  /**
   * JsArray extensions
   */
  private object JsArrays {
    def init(): Unit = {
      // Returns the length of the given array: JSON([1 to 5]).length()
      jsArrayFunction0(name = "length", _.elements.length, returnType_? = INT_TYPE)

      // Returns the length of the given array: JSON([1 to 5]).size()
      jsArrayFunction0(name = "size", _.elements.length, returnType_? = INT_TYPE)
    }

    private def jsArrayFunction0(name: String, f: JsArray => Any, returnType_? : Option[ColumnType]): Unit = {
      registerVirtualMethod(VirtualMethod(_.isInstanceOf[JsArray], fx = NamedFunction(
        name = name,
        params = Seq("array Any".c),
        code = new JsArrayFunction0(name, "array".f, f),
        returnType_? = returnType_?
      )))
    }

  }

  /**
   * Numeric extensions
   */
  private object Numerics {

    def init(): Unit = {
      // Number op Number
      numberFunction1F(name = "$amp", (aa, bb, _) => aa.longValue() & bb.longValue(), returnType_? = NUMERIC_TYPE)
      numberFunction1F(name = "$bar", (aa, bb, _) => aa.longValue() | bb.longValue(), returnType_? = NUMERIC_TYPE)
      numberFunction1F(name = "$div", (aa, bb, op) => {
        if (bb.doubleValue() == 0) op.dieDivisionByZero(s"$aa / $bb") else aa.doubleValue() / bb.doubleValue()
      }, returnType_? = NUMERIC_TYPE)
      numberFunction1F(name = "$greater$greater", (aa, bb, _) => aa.longValue() >> bb.longValue(), returnType_? = NUMERIC_TYPE)
      numberFunction1F(name = "$less$less", (aa, bb, _) => aa.longValue() << bb.longValue(), returnType_? = NUMERIC_TYPE)
      numberFunction1F(name = "$minus", (aa, bb, _) => aa.doubleValue() - bb.doubleValue(), returnType_? = NUMERIC_TYPE)
      numberFunction1F(name = "$percent", (aa, bb, _) => aa.longValue() % bb.longValue(), returnType_? = NUMERIC_TYPE)
      numberFunction1F(name = "$plus", (aa, bb, _) => aa.doubleValue() + bb.doubleValue(), returnType_? = NUMERIC_TYPE)
      numberFunction1F(name = "$times", (aa, bb, _) => aa.doubleValue() * bb.doubleValue(), returnType_? = NUMERIC_TYPE)
      numberFunction1F(name = "$times$times", (aa, bb, _) => Math.pow(aa.doubleValue(), bb.doubleValue()), returnType_? = NUMERIC_TYPE)
      numberFunction1F(name = "$up", (aa, bb, _) => aa.longValue() ^ bb.longValue(), returnType_? = NUMERIC_TYPE)

      // Returns the binary representation of a number: 33.asBinaryString()
      numberFunction0(name = "asBinaryString", n => Integer.toBinaryString(n.intValue()), returnType_? = STRING_TYPE)

      // Returns a string with the margin removed: "|Hello World".stripMargin('|')
      numberFunction1F(name = "toScale", (number, scale, _) => {
        val factor = Math.pow(10, scale.intValue())
        (number.doubleValue() * factor).longValue() / factor
      }, returnType_? = NUMERIC_TYPE)
    }

    private def numberFunction0(name: String, f: Number => Any, returnType_? : Option[ColumnType]): Unit = {
      registerVirtualMethod(VirtualMethod(_.isInstanceOf[Number], fx = NamedFunction(
        name = name,
        params = Seq("value Numeric".c),
        code = new NumberFunction0(name, "value".f, f),
        returnType_? = returnType_?
      )))
    }

    private def numberFunction1F(name: String, f: (Number, Number, Instruction) => Number, returnType_? : Option[ColumnType]): Unit = {
      registerVirtualMethod(VirtualMethod(_.isInstanceOf[Number], fx = NamedFunction(
        name = name,
        params = Seq("value1 Numeric".c, "value2 Numeric".c),
        code = new NumberFunction1F(name, "value1".f, "value2".f, f),
        returnType_? = returnType_?
      )))
    }

  }

  /**
   * String extensions
   */
  private object Strings {
    import com.lollypop.runtime.ModelsJsonProtocol._

    def init(): Unit = {
      // Parses a JSON-formatted string returning the object representation: "Hello World".fromJson()
      stringFunction0(name = "fromJson", _.fromJSON[Map[String, Any]], returnType_? = ANY_TYPE)

      // Parses a Hex-encoded string returning the byte array: "dead00beef01a3f5".fromHex()
      stringFunction0(name = "fromHex", s => Hex.decodeHex(s.filterNot(_ == '.')), returnType_? = BYTE_ARRAY_TYPE)

      // Returns the tail of the string: "Welcome".head()
      stringFunction0(name = "head", _.head, returnType_? = CHAR_TYPE)

      // Returns the tail of the string: "cool".headOption()
      stringFunction0(name = "headOption", _.headOption, returnType_? = CHAR_TYPE)

      // Returns the index of the value if found in the collection: "Accept-Code".indexOf('C')
      stringFunction1[Any](name = "indexOf", (arr, v) => arr.toSeq.indexOf(v), returnType_? = INT_TYPE)

      // Returns a string in reverse order: "Hello World".reverse()
      stringFunction0(name = "reverse", _.reverse, returnType_? = STRING_TYPE)

      // Returns a string with the margin removed: "HelloWorld".forall(c => Character.isAlphabetic(c))
      stringFunction1[Any](name = "forall", {
        case (s, f: AnonymousFunction) => safeCast[Char => Boolean](f.toScala).exists(s.forall)
        case (_, x) => dieIllegalType(x)
      }, returnType_? = BOOLEAN_TYPE)

      // Returns a string with the margin removed: "|Hello World".stripMargin('|')
      stringFunction1[Char](name = "stripMargin", (s, c) => s.stripMargin(c), returnType_? = STRING_TYPE)

      // Returns the concatenation of two strings: "Hello" + "World"
      stringFunction1[Any](name = "$plus", (a, b) => a + b, returnType_? = STRING_TYPE)

      // Returns the repetition of a string: "Hello" * 5
      stringFunction1[Any](name = "$times", {
        case (s, n: String) if n.matches("\\d+") => s * n.toInt
        case (s, n: Number) => s * n.intValue()
        case (_, x) => dieIllegalType(x)
      }, returnType_? = STRING_TYPE)
    }

    private def stringFunction0(name: String, f: String => Any, returnType_? : Option[ColumnType]): Unit = {
      registerVirtualMethod(VirtualMethod(`class` = classOf[String], fx = NamedFunction(
        name = name,
        params = Seq("value String".c),
        code = new StringFunction0(name, "value".f, f),
        returnType_? = returnType_?
      )))
    }

    private def stringFunction1[A](name: String, f: (String, A) => Any, returnType_? : Option[ColumnType]): Unit = {
      registerVirtualMethod(VirtualMethod(`class` = classOf[String], fx = NamedFunction(
        name = name,
        params = Seq("value String".c, "arg1 Any".c),
        code = new StringFunction1(name, "value".f, "arg1".f, f),
        returnType_? = returnType_?
      )))
    }

  }

  ////////////////////////////////////////////////////////////////////////////////
  //  Initialization Function
  ////////////////////////////////////////////////////////////////////////////////

  def init(): Unit = {
    Anything.init()
    Arrays.init()
    ByteArrays.init()
    Dates.init()
    Durations.init()
    JsArrays.init()
    Numerics.init()
    Strings.init()
  }

  ////////////////////////////////////////////////////////////////////////////////
  //  Mix-ins
  ////////////////////////////////////////////////////////////////////////////////

  private trait ZeroArguments extends Instruction {

    def name: String

    def value: Expression

    override def toSQL: String = s"${value.toSQL}.$name()"
  }

  private trait OneArgument extends ZeroArguments {

    def arg1: Expression

    override def toSQL: String = s"${value.toSQL}.$name(${arg1.toSQL})"
  }

  private trait TwoArguments extends OneArgument {

    def arg2: Expression

    override def toSQL: String = {
      Seq(value.toSQL, name, Seq(arg1, arg2).map(_.toSQL).mkString("(", ", ", ")")).mkString
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  //  Any Function implementations
  ////////////////////////////////////////////////////////////////////////////////

  private class AnyFunction0(val name: String, val value: Expression, f: Any => Any) extends RuntimeExpression with ZeroArguments {
    override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
      (scope, IOCost.empty, value.asAny.map(f).orNull)
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  //  Array Function implementations
  ////////////////////////////////////////////////////////////////////////////////

  private class ArrayFunction0(val name: String, val value: Expression, f: Array[_] => Any) extends RuntimeExpression with ZeroArguments {
    override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
      (scope, IOCost.empty, (for {array <- value.asArray} yield f(array)).orNull)
    }
  }

  private class ArrayFunction1[A](val name: String, val value: Expression, val arg1: Expression, f: (Array[_], A) => Any)
    extends RuntimeExpression with OneArgument {
    override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
      (scope, IOCost.empty, (for {
        array <- value.asArray
        arg1 <- arg1.asAny.flatMap(safeCast[A])
      } yield f(array, arg1)).orNull)
    }
  }

  private class ArrayFunction1S[A](val name: String, val value: Expression, val arg1: Expression, f: (Array[_], A, Scope) => Any)
    extends RuntimeExpression with OneArgument {
    override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
      (scope, IOCost.empty, (for {
        array <- value.asArray
        arg1 <- arg1.asAny.flatMap(safeCast[A])
      } yield f(array, arg1, scope)).orNull)
    }
  }

  private class ArrayFunction2[A, B](val name: String, val value: Expression, val arg1: Expression, val arg2: Expression, f: (Array[_], A, B) => Any)
    extends RuntimeExpression with TwoArguments {
    override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
      (scope, IOCost.empty, (for {
        array <- value.asArray
        arg1 <- arg1.asAny.flatMap(safeCast[A])
        arg2 <- arg2.asAny.flatMap(safeCast[B])
      } yield f(array, arg1, arg2)).orNull)
    }
  }

  private class ArrayFunction2S[A, B](val name: String, val value: Expression, val arg1: Expression, val arg2: Expression, f: (Array[_], A, B, Scope) => Any)
    extends RuntimeExpression with TwoArguments {
    override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
      (scope, IOCost.empty, (for {
        array <- value.asArray
        arg1 <- arg1.asAny.flatMap(safeCast[A])
        arg2 <- arg2.asAny.flatMap(safeCast[B])
      } yield f(array, arg1, arg2, scope)).orNull)
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  //  ByteArray Function implementations
  ////////////////////////////////////////////////////////////////////////////////

  private class BytesFunction0(val name: String, val value: Expression, f: Array[Byte] => Any)
    extends RuntimeExpression with ZeroArguments {
    override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
      (scope, IOCost.empty, value.asByteArray.map(f).orNull)
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  //  Date Function implementations
  ////////////////////////////////////////////////////////////////////////////////

  private class DateFormat(val value: Expression, formatString: Expression) extends RuntimeExpression with ZeroArguments {
    override val name = "format"

    override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
      (scope, IOCost.empty, (for {
        value <- value.asDateTime
        formatString <- formatString.asString
      } yield new SimpleDateFormat(formatString).format(value)).orNull)
    }
  }

  private class DateMinus(val value: Expression, val arg1: Expression) extends RuntimeExpression with OneArgument {
    override val name = "$minus"

    override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
      val (s1, c1, v1) = LollypopVM.execute(scope, value)
      val (s2, c2, v2) = LollypopVM.execute(s1, arg1)
      (s2, c1 ++ c2, (for {
        date <- Option(DateTimeType.convert(v1))
        result <- Option(v2) map {
          case f: FiniteDuration => date - f
          case d: Date => date - d
          case n: Number => date - n.longValue().millis
          case x => arg1.dieIllegalType(x)
        }
      } yield result).orNull)
    }
  }

  private class DateFunction0(val name: String, val value: Expression, f: Date => Any) extends RuntimeExpression with ZeroArguments {
    override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
      val (s, c, r) = LollypopVM.execute(scope, value)
      (s, c, r match {
        case d: Date => f(d)
        case x => value.dieIllegalType(x)
      })
    }
  }

  private class DateFunction1[A](name: String, value: Expression, arg1: Expression, f: (Date, A) => Any)
    extends RuntimeExpression {
    override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
      (scope, IOCost.empty, (for {
        arg0 <- value.asDateTime
        arg1 <- arg1.asAny.flatMap(safeCast[A])
      } yield f(arg0, arg1)).orNull)
    }

    override def toSQL: String = s"${value.toSQL}.$name(${arg1.toSQL})"
  }

  private class DurationFunction1[A](val name: String, val value: Expression, val arg1: Expression, f: (FiniteDuration, A) => Any)
    extends RuntimeExpression with OneArgument {
    override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
      (scope, IOCost.empty, (for {
        arg0 <- value.asInterval
        arg1 <- arg1.asAny.flatMap(safeCast[A])
      } yield f(arg0, arg1)).orNull)
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  //  JavaScript Array Function implementations
  ////////////////////////////////////////////////////////////////////////////////

  private class JsArrayFunction0(val name: String, val value: Expression, f: JsArray => Any) extends RuntimeExpression with ZeroArguments {
    override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
      (scope, IOCost.empty, (for {array <- value.asJsArray} yield f(array)).orNull)
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  //  Number Function implementations
  ////////////////////////////////////////////////////////////////////////////////

  private class NumberFunction0(val name: String, val value: Expression, f: Number => Any) extends RuntimeExpression with ZeroArguments {
    override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
      (scope, IOCost.empty, value.asNumeric.map(f).orNull)
    }
  }

  private class NumberFunction1F(val name: String, val value: Expression, val arg1: Expression, f: (Number, Number, Instruction) => Any)
    extends RuntimeExpression with OneArgument {
    override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
      (scope, IOCost.empty, (for {
        aa <- value.asNumeric
        bb <- arg1.asNumeric
        _type = Inferences.resolveType(Seq(aa, bb).map(Inferences.fromValue): _*)
      } yield _type.convert(f(aa, bb, value))).orNull)
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  //  String Function implementations
  ////////////////////////////////////////////////////////////////////////////////

  private class StringFunction0(val name: String, val value: Expression, f: String => Any) extends RuntimeExpression with ZeroArguments {
    override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
      (scope, IOCost.empty, value.asString.map(f).orNull)
    }
  }

  private class StringFunction1[A](val name: String, val value: Expression, val arg1: Expression, f: (String, A) => Any)
    extends RuntimeExpression with OneArgument {
    override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
      (scope, IOCost.empty, (for {
        arg0 <- value.asString
        arg1 <- arg1.asAny.flatMap(safeCast[A])
      } yield f(arg0, arg1)).orNull)
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  //  ToTable Function implementations
  ////////////////////////////////////////////////////////////////////////////////

  private class ToTable(val value: Expression) extends RuntimeQueryable with ZeroArguments {
    override val name = "toTale"

    override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
      val (scope1, cost1, result1) = LollypopVM.execute(scope, value)
      val rc1 = result1.normalize match {
        case t: TableRendering => t.toTable
        case a: Array[_] => convertToTable(a)
        case s: Seq[_] => convertToTable(s)
        case s: Scope => s.toRowCollection
        case t: TableRendering => t.toTable
        case p: Product => p.toRowCollection
        case m: QMap[_, _] => m.toKeyValueCollection
        case x => dieIllegalType(x)
      }
      (scope1, cost1, rc1)
    }
  }

}