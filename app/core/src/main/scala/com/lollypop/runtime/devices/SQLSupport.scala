package com.lollypop.runtime.devices

import com.lollypop.language.models.{Condition, Expression, FieldRef, OrderColumn}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.devices.SQLSupport.SelectBuilder
import com.lollypop.runtime.instructions.functions.NS
import com.lollypop.runtime.instructions.queryables.{RuntimeQueryable, Select}
import lollypop.io.IOCost

/**
 * SQL Support
 */
trait SQLSupport { self: RowCollection =>

  def select(fields: Expression*): SelectBuilder = {
    new SelectBuilder(self, fields)
  }

}

/**
 * SQL Support Companion
 */
object SQLSupport {

  /**
   * SQL Builder
   * @param collection the [[RowCollection block device]]
   * @param fields     the selection [[Expression fields]]
   */
  class SelectBuilder(val collection: RowCollection, val fields: Seq[Expression]) extends RuntimeQueryable {
    private var select: Select = Select(fields = fields, from = Some(WrappedQueryable(collection)))

    override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = select.execute()

    def groupBy(groupFields: FieldRef*): this.type = {
      this.select = select.copy(groupBy = groupFields)
      this
    }

    def having(condition: Condition): this.type = {
      this.select = select.copy(having = Some(condition))
      this
    }

    def limit(maxResults: Expression): this.type = limit(maxResults = Option(maxResults))

    def limit(maxResults: Option[Expression]): this.type = {
      this.select = select.copy(limit = maxResults)
      this
    }

    def toModel: Select = select

    def orderBy(orderBy: OrderColumn*): this.type = {
      this.select = select.copy(orderBy = orderBy)
      this
    }

    def where(condition: Condition): this.type = where(condition = Option(condition))

    def where(condition: Option[Condition]): this.type = {
      this.select = select.copy(where = condition)
      this
    }

  }

  /**
   * A device wrapped as a [[RuntimeQueryable]]
   * @param device the [[RowCollection device]]
   */
  private case class WrappedQueryable(device: RowCollection) extends RuntimeQueryable {
    override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = (scope, IOCost.empty, device)

    override def toSQL: String = NS(device.ns).toSQL
  }

}


