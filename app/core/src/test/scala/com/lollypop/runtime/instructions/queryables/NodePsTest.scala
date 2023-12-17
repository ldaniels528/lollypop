package com.lollypop.runtime.instructions.queryables

import com.lollypop.runtime.{Scope, _}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class NodePsTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[NodePs].getSimpleName) {

    it("should provide information about active peers") {
      val (_, _, device) =
        """|nodeA = Nodes.start()
           |nodeB = Nodes.start()
           |nodeA.awaitStartup(Duration('1 second'))
           |nodeB.awaitStartup(Duration('1 second'))
           |result = nps
           |nodeA.stop()
           |nodeB.stop()
           |result
           |""".stripMargin.searchSQL(Scope())
      device.tabulate().foreach(logger.info)
      assert(device.getLength == 2)
    }

  }

}
