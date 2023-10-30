package lollypop.io

import com.lollypop.die
import com.lollypop.runtime.ROWID
import com.lollypop.runtime.datatypes.TableType
import com.lollypop.runtime.instructions.expressions.TableExpression
import com.lollypop.runtime.instructions.queryables.ProductTableRendering
import com.lollypop.util.OptionHelper.OptionEnrichment
import com.lollypop.util.StringRenderHelper.StringRenderer

/**
 * Represents the cost of an I/O operation
 * @param altered   the number of altered resources
 * @param created   the number of created resources
 * @param destroyed the number of destroyed resources
 * @param deleted   the number of deleted resources
 * @param inserted  the number of inserted rows
 * @param matched   the number of matched rows
 * @param scanned   the number of scanned rows
 * @param shuffled  the number of updated index rows
 * @param updated   the number of updated rows
 * @param rowIDs    the IDs of the [[RowIDRange inserted rows]]
 */
case class IOCost(altered: Int = 0,
                  created: Int = 0,
                  destroyed: Int = 0,
                  deleted: Int = 0,
                  inserted: Int = 0,
                  matched: Int = 0,
                  scanned: Int = 0,
                  shuffled: Int = 0,
                  updated: Int = 0,
                  rowIDs: RowIDRange = RowIDRange())
  extends ProductTableRendering with TableExpression {

  def ++(that: IOCost): IOCost = this.copy(
    rowIDs = rowIDs ++ that.rowIDs,
    altered = altered + that.altered,
    created = created + that.created,
    destroyed = destroyed + that.destroyed,
    deleted = deleted + that.deleted,
    inserted = inserted + that.inserted,
    matched = matched + that.matched,
    scanned = scanned + that.scanned,
    shuffled = shuffled + that.shuffled,
    updated = updated + that.updated
  )

  def firstRowID: ROWID = rowIDs.headOption || die("No row ID was returned")

  def getUpdateCount: Int = List(altered, created, deleted, destroyed, inserted, updated).sum

  def isEmpty: Boolean = {
    rowIDs.isEmpty &&
      Seq(altered, created, destroyed, deleted, inserted, matched, scanned, shuffled, updated).forall(_ == 0)
  }

  def nonEmpty: Boolean = !isEmpty

  override def returnType: TableType = toTableType

  def toMap: Map[String, Long] = {
    Map[String, Long]((this.getClass.getDeclaredFields.map(_.getName) zip this.productIterator).flatMap {
      case ("rowIDs", range: RowIDRange) => range.start.map("rowIdStart" -> _).toList ::: range.end.map("rowIdEnd" -> _).toList
      case (name, value: Int) => List(name -> value.toLong)
    }: _*)
  }

  override def toString: String = toMap.renderAsJson

}

object IOCost {
  val empty: IOCost = IOCost()

  def apply(mapping: Map[String, Long]): IOCost = {
    mapping.foldLeft(IOCost()) {
      case (cost, ("altered", value)) => cost.copy(altered = value.toInt)
      case (cost, ("created", value)) => cost.copy(created = value.toInt)
      case (cost, ("destroyed", value)) => cost.copy(destroyed = value.toInt)
      case (cost, ("deleted", value)) => cost.copy(deleted = value.toInt)
      case (cost, ("inserted", value)) => cost.copy(inserted = value.toInt)
      case (cost, ("matched", value)) => cost.copy(matched = value.toInt)
      case (cost, ("scanned", value)) => cost.copy(scanned = value.toInt)
      case (cost, ("shuffled", value)) => cost.copy(shuffled = value.toInt)
      case (cost, ("updated", value)) => cost.copy(updated = value.toInt)
      case (cost, ("rowIdStart", value)) => cost.copy(rowIDs = cost.rowIDs.copy(start = Some(value)))
      case (cost, ("rowIdEnd", value)) => cost.copy(rowIDs = cost.rowIDs.copy(end = Some(value)))
      case (cost, _) => cost
    }
  }

}