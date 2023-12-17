package lollypop.io

import com.lollypop.runtime._
import com.lollypop.util.StringRenderHelper.StringRenderer
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class IOCostTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[IOCost].getSimpleName) {

    it("should combine metrics") {
      val actual = List(
        IOCost(destroyed = 1),
        IOCost(created = 1),
        IOCost(inserted = 5),
        IOCost(scanned = 5),
        IOCost(scanned = 3, matched = 1)) reduce (_ ++ _)
      actual.toRowCollection.tabulate().foreach(logger.info)
      assert(actual == IOCost(destroyed = 1, created = 1, inserted = 5, scanned = 8, matched = 1))
    }

    it("should return the contents as Map") {
      val cost = IOCost(destroyed = 1, created = 1, inserted = 5, scanned = 5, matched = 1)
      assert(cost.toMap == Map(
        "shuffled" -> 0, "matched" -> 1, "updated" -> 0, "destroyed" -> 1, "scanned" -> 5,
        "inserted" -> 5, "altered" -> 0, "deleted" -> 0, "created" -> 1
      ))
    }

    it("should encode/decode itself") {
      val cost0 = IOCost(inserted = 5, rowIDs = RowIDRange((0L to 5L).toList))
      val mapping = cost0.toMap
      logger.info(s"mapping: ${mapping.renderAsJson}")
      val cost1 = IOCost(mapping)
      assert(cost0 == cost1)
    }

  }

}
