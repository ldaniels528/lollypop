package com.lollypop.runtime.datatypes

import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.{LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * SequenceNumberType Tests
 */
class RowNumberTypeTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val ctx: LollypopUniverse = LollypopUniverse()

  describe(RowNumberType.getClass.getSimpleName) {

    it("it should represent the record ID") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|namespace 'samples.test'
           |drop if exists SequenceNumberTypeTest
           |create table SequenceNumberTypeTest (
           |  id RowNumber,
           |  value String(255)
           |)
           |
           |insert into SequenceNumberTypeTest (value)
           |values ('Hello'), ('Bon jour'), ('Buena dias')
           |
           |insert into SequenceNumberTypeTest (id, value) values (55, 'Guten Morgen')
           |
           |select * from SequenceNumberTypeTest
           |""".stripMargin
      )
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(
        Map("id" -> 0, "value" -> "Hello"),
        Map("id" -> 1, "value" -> "Bon jour"),
        Map("id" -> 2, "value" -> "Buena dias"),
        Map("id" -> 3, "value" -> "Guten Morgen")
      ))
      assert(device.recordSize == 270)
    }

  }

}
