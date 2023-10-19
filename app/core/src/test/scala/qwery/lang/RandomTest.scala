package qwery.lang

import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class RandomTest extends AnyFunSpec with VerificationTools {

  describe(Random.getClass.getSimpleName) {

    it("should execute: Random.nextDouble()") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|Random.nextDouble()
           |""".stripMargin)
      assert(Option(result).collect { case n: Double => n }.nonEmpty)
    }

    it("should execute: Random.nextDouble(1000)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|Random.nextDouble(1000)
           |""".stripMargin)
      val value = Option(result).collect { case n: Double => n }
      assert(value.exists(_ <= 1000))
    }

    it("should execute: Random.nextInt()") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|Random.nextInt()
           |""".stripMargin)
      assert(Option(result).collect { case n: Int => n }.nonEmpty)
    }

    it("should execute: Random.nextInt(5000)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|Random.nextInt(5000)
           |""".stripMargin)
      val value = Option(result).collect { case n: Int => n }
      assert(value.exists(_ < 5000))
    }

    it("should execute: Random.nextLong()") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|Random.nextLong()
           |""".stripMargin)
      assert(Option(result).collect { case n: Long => n }.nonEmpty)
    }

    it("should execute: Random.nextLong(12345)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|Random.nextLong(12345)
           |""".stripMargin)
      val value = Option(result).collect { case n: Long => n }
      assert(value.exists(_ < 12345))
    }

    it("should execute: Random.nextString(['A' to 'Z'], 8)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|Random.nextString(['A' to 'Z'], 8)
           |""".stripMargin)
      val string = Option(result).collect { case s: String => s }
      assert(string.exists(s => s.length == 8 && s.toCharArray.forall(_.isLetter)))
    }

  }

}
