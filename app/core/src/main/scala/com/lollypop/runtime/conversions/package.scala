package com.lollypop.runtime

import com.lollypop.language.models.{Expression, Instruction}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.QMap
import lollypop.io.IOCost

import java.io.{InputStream, OutputStream}
import scala.concurrent.duration.FiniteDuration

package object conversions {

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
    def pull[A](f: Any => A)(implicit scope: Scope): (Scope, IOCost, A) = {
      instruction.execute(scope) ~>> f
    }

    /**
     * Evaluates an expression converting the result to an array value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and an [[Array]].
     */
    @inline
    def pullArray(implicit scope: Scope): (Scope, IOCost, Array[_]) = {
      instruction.execute(scope) ~>> ArrayType.convert
    }

    /**
     * Evaluates an expression converting the result to an array value.
     * @param f     the `Array[_]` to `Any` transformation function
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and an `A`.
     */
    @inline
    def pullArray[A](f: Array[_] => A)(implicit scope: Scope): (Scope, IOCost, A) = {
      pullArray(scope) ~>> f
    }

    /**
     * Evaluates an expression converting the result to an boolean value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[Boolean]].
     */
    @inline
    def pullBoolean(implicit scope: Scope): (Scope, IOCost, Boolean) = {
      instruction.execute(scope) ~>> BooleanType.convert
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
      pullByteArray(scope) ~>> f
    }

    /**
     * Evaluates an expression converting the result to a date value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[java.util.Date]].
     */
    @inline
    def pullDate(implicit scope: Scope): (Scope, IOCost, java.util.Date) = {
      instruction.execute(scope) ~>> DateTimeType.convert
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
      pullDate(scope) ~>> f
    }

    /**
     * Evaluates an expression converting the result to a date value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[java.util.Date]].
     */
    @inline
    def pullDictionary(implicit scope: Scope): (Scope, IOCost, QMap[String, Any]) = {
      instruction.execute(scope) ~>> DictionaryConversion.convert
    }

    /**
     * Evaluates an expression converting the result to a double value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[Double]].
     */
    @inline
    def pullDouble(implicit scope: Scope): (Scope, IOCost, Double) = {
      instruction.execute(scope) ~>> Float64Type.convert
    }

    /**
     * Evaluates an expression converting the result to a duration value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[FiniteDuration]].
     */
    @inline
    def pullDuration(implicit scope: Scope): (Scope, IOCost, FiniteDuration) = {
      instruction.execute(scope) ~>> DurationType.convert
    }

    /**
     * Evaluates an expression converting the result to a float value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[Float]].
     */
    @inline
    def pullFloat(implicit scope: Scope): (Scope, IOCost, Float) = {
      instruction.execute(scope) ~>> Float32Type.convert
    }

    /**
     * Evaluates and converts the [[Expression]] into an [[InputStream]]
     * @return a tuple of the [[Scope]], [[IOCost]] and [[InputStream]]
     */
    @inline
    def pullInputStream(implicit scope: Scope): (Scope, IOCost, InputStream) = {
      instruction.execute(scope) ~>> InputStreamConversion.convert
    }

    /**
     * Evaluates and converts the [[Expression]] into an [[OutputStream]]
     * @return a tuple of the [[Scope]], [[IOCost]] and [[OutputStream]]
     */
    @inline
    def pullOutputStream(implicit scope: Scope): (Scope, IOCost, OutputStream) = {
      instruction.execute(scope) ~>> OutputStreamConversion.convert
    }

    /**
     * Evaluates an expression converting the result to a [[Int]] value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[Int]].
     */
    @inline
    def pullInt(implicit scope: Scope): (Scope, IOCost, Int) = {
      instruction.execute(scope) ~>> Int32Type.convert
    }

    /**
     * Evaluates an expression converting the result to a [[Number]] value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[Number]].
     */
    @inline
    def pullNumber(implicit scope: Scope): (Scope, IOCost, Number) = {
      instruction.execute(scope) ~>> NumericType.convert
    }

    /**
     * Evaluates an expression converting the result to a [[Number]] value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a `A`.
     */
    @inline
    def pullNumber[A](f: Number => A)(implicit scope: Scope): (Scope, IOCost, A) = {
      pullNumber(scope) ~>> f
    }

    /**
     * Evaluates an expression converting the result to a [[String]] value.
     * @param scope the [[Scope scope]]
     * @return a tuple containing the updated [[Scope]], [[IOCost]] and a [[String]].
     */
    @inline
    def pullString(implicit scope: Scope): (Scope, IOCost, String) = {
      instruction.execute(scope) ~>> StringType.convert
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
      pullString(scope) ~>> f
    }

  }

}
