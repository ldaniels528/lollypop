package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.HelpDoc.{CATEGORY_UNCLASSIFIED, PARADIGM_FUNCTIONAL}
import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.LollypopVM.implicits.LollypopVMSQL
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.instructions.functions.{FunctionCallParserE0, InternalFunctionCall}
import com.lollypop.runtime.instructions.queryables.LollypopComponentsTest.lootBoxValues
import lollypop.io.IOCost
import lollypop.lang.Random
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import java.io.File

class LollypopComponentsTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[LollypopComponents].getSimpleName) {

    it("should install components via File") {
      implicit val ctx: LollypopUniverse = LollypopUniverse(isServerMode = true)
      val opcodesFile = new File("app") / "core" / "src" / "test" / "resources" / "lootBox.txt"
      val (sa, _, rc) =
        s"""|lollypopComponents(file)
            |""".stripMargin.searchSQL(ctx.createRootScope().withVariable(name = "file", value = opcodesFile))
      rc.tabulate().foreach(logger.info)

      val (_, _, vb) =
        s"""|lootBox()
            |""".stripMargin.executeSQL(sa)
      logger.info(s"received item '$vb'")
      assert(lootBoxValues contains vb)
    }

    it("should install components via String") {
      implicit val ctx: LollypopUniverse = LollypopUniverse(isServerMode = true)
      val (sa, _, rc) =
        s"""|lollypopComponents(
            |'''|com.lollypop.runtime.instructions.queryables.LollypopComponentsTest$$LootBox$$
            |   |'''.stripMargin('|'))
            |""".stripMargin.searchSQL(ctx.createRootScope())
      rc.tabulate().foreach(logger.info)

      val (_, _, vb) =
        s"""|lootBox()
            |""".stripMargin.executeSQL(sa)
      logger.info(s"received item '$vb'")
      assert(lootBoxValues contains vb)
    }

    it("should install components via URL") {
      implicit val ctx: LollypopUniverse = LollypopUniverse(isServerMode = true)
      val (sa, _, rc) =
        s"""|import implicit "com.lollypop.util.StringHelper$$StringEnrichment"
            |lollypopComponents("/lootBox.txt".toResourceURL())
            |""".stripMargin.searchSQL(ctx.createRootScope())
      rc.tabulate().foreach(logger.info)

      val (_, _, vb) =
        s"""|lootBox()
            |""".stripMargin.executeSQL(sa)
      logger.info(s"received item '$vb'")
      assert(lootBoxValues contains vb)
    }

  }

}

object LollypopComponentsTest {
  private val lootBoxValues = Seq("Gold", "Silver", "Bronze", "Lead", "Steel", "Iron", "Junk").reverse

  case class LootBox() extends InternalFunctionCall with RuntimeExpression {
    override def execute()(implicit scope: Scope): (Scope, IOCost, String) = {
      (scope, IOCost.empty, lootBoxValues(Random.nextInt(lootBoxValues.size)))
    }
  }

  object LootBox extends FunctionCallParserE0(
    name = "lootBox",
    description = "A randomized loot box",
    example = "lootBox()",
    category = CATEGORY_UNCLASSIFIED,
    paradigm = PARADIGM_FUNCTIONAL)

}