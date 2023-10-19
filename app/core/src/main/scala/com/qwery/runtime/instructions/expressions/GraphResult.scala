package com.qwery.runtime.instructions.expressions

import com.qwery.runtime.devices.RowCollection

/**
 * Represents a chart-based drawing result
 * @param options the chart options
 * @param data    the chart [[RowCollection data]]
 */
case class GraphResult(options: Map[String, Any], data: RowCollection)
  extends AbstractTableExpression(data.columns)