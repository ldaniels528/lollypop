package com.lollypop.util

import com.lollypop.util.StringHelper.StringEnrichment
import com.lollypop.util.StringRenderHelper._

/**
 * Tabulator is an ASCII table generator
 * @example {{{
 * |------------------------------|
 * | symbol | exchange | lastSale |
 * |------------------------------|
 * | XYZ    | AMEX     |    31.95 |
 * | AAXX   | NYSE     |    56.12 |
 * | JUNK   | AMEX     |    97.61 |
 * |------------------------------|
 * }}}
 */
object Tabulator {

  /**
   * Produces an ASCII table containing the column headers and row data
   * @param headers  the column headers
   * @param rows     the row data
   * @param limit    the maximum number of results
   * @param renderer the cell renderer
   * @return an array of strings representing the table
   */
  def tabulate(headers: List[String],
               rows: List[List[Option[Any]]],
               limit: Int = Int.MaxValue,
               renderer: Any => String = _.renderTable.limit(100)): List[String] = {
    if (rows.isEmpty) Nil else {
      // cache the column names and row values as strings
      val values = rows.take(limit).map(_.map(x => renderer(x.orNull)))
      val isNumeric = for {
        first_row <- rows.headOption.toList
        value_? <- first_row
      } yield value_?.exists(_.asInstanceOf[AnyRef].isInstanceOf[Number])

      // for each column and respective value determine the max width
      val columnWidths: List[Int] = {
        val widths = values map { row => (headers zip row).map { case (n, v) => n.length max v.length } }
        widths.headOption.toList flatMap { first =>
          widths.tail.foldLeft[List[Int]](first) { (widths, row) => (widths zip row).map { case (c, v) => c max v } }
        }
      }

      // define the horizontal bar
      val fullWidth = columnWidths.map(_ + 3).sum - 1
      val horizontalBar = "|" + "-" * fullWidth + "|"

      // build the table
      var lines: List[String] = Nil
      lines = horizontalBar :: lines
      lines = "| " + (headers zip columnWidths).map { case (s, w) => s.padTo(w, ' ') }.mkString(" | ") + " |" :: lines
      lines = horizontalBar :: lines
      lines = values.map { row =>
        "| " + (row zip columnWidths zip isNumeric).map {
          case ((s, w), isNum) if isNum => s.reverse.padTo(w, ' ').reverse
          case ((s, w), _) => s.padTo(w, ' ')
        }.mkString(" | ") + " |"
      }.reverse ::: lines
      lines = horizontalBar :: lines
      lines.reverse
    }
  }

}