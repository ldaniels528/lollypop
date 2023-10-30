package com.lollypop.runtime.instructions.expressions

import com.lollypop.runtime.devices.RowCollection

/**
 * Represents a chart-based drawing result
 * @param options the chart options
 * @param data    the chart [[RowCollection data]]
 */
case class GraphResult(options: Map[String, Any], data: RowCollection)
  extends AbstractTableExpression(data.columns)