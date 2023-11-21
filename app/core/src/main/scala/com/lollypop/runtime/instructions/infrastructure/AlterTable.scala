package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.HelpDoc.{CATEGORY_DATAFRAMES_INFRA, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models.{Atom, Column, Instruction}
import com.lollypop.runtime.conversions.ExpressiveTypeConversion
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollectionZoo._
import com.lollypop.runtime.devices.TableColumn
import com.lollypop.runtime.devices.TableColumn.implicits.{SQLToColumnConversion, TableColumnSeq}
import com.lollypop.runtime.instructions.infrastructure.AlterTable.Alteration
import com.lollypop.runtime.instructions.queryables.TableVariableRef
import com.lollypop.runtime.{DatabaseObjectRef, ROWID_NAME, ResourceManager, Scope}
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

import java.io.{File, FileInputStream, FileOutputStream}
import scala.language.postfixOps

/**
 * Represents an alter table statement
 * @example {{{
 * alter table stocks add column comments: String = ""
 * }}}
 * @example {{{
 * alter table stocks drop column comments
 * }}}
 * @param ref         the [[DatabaseObjectRef table reference]]
 * @param alterations the collection of [[Alteration alterations]]
 */
case class AlterTable(ref: DatabaseObjectRef, alterations: Seq[Alteration]) extends RuntimeModifiable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    import AlterTable._

    // lookup the table
    val host = scope.getRowCollection(ref)

    // determine the new column list
    val newColumns = alterations.foldLeft[List[TableColumn]](host.columns.toList) {
      case (columns, AddColumn(column)) => columns ::: column.toTableColumn :: Nil
      case (columns, AppendColumn(column)) => columns ::: column.toTableColumn :: Nil
      case (columns, DropColumn(columnName)) =>
        columns.getIndexByName(columnName) ~> (n => columns.slice(0, n) ::: columns.slice(n + 1, columns.length))
      case (columns, PrependColumn(column)) => column.toTableColumn :: columns
      case (columns, RenameColumn(oldName, newName)) =>
        columns.getIndexByName(oldName) ~> (n => columns.slice(0, n) ::: columns(n).copy(name = newName) :: columns.slice(n + 1, columns.length))
      case (columns, _: SetLabel) => columns
      case (_, alteration) => dieUnsupportedEntity(alteration, entityName = "alteration")
    }

    // create the field renaming mapping
    val renaming = Map(alterations.collect { case RenameColumn(oldName, newName) => oldName -> newName }: _*)

    // duplicate the device with the modifications
    val newDevice = createTempTable(newColumns)
    val cost = host.foldLeft(IOCost()) { case (agg, row) =>
      val newRow = row.toMap flatMap {
        case (name, _) if name == ROWID_NAME => None
        case (name, value) if renaming.contains(name) => Some(renaming(name) -> value)
        case (name, value) => Some(name -> value)
      }
      agg ++ newDevice.insert(newRow.toRow(newDevice))
    }

    // is the host an in-memory table?
    ref match {
      case TableVariableRef(name) =>
        (scope.withVariable(name, newDevice.returnType, newDevice, isReadOnly = false), cost, cost)
      case _ =>
        // swap the files
        val srcFile = host.ns.tableDataFile
        val tempFile = newDevice.ns.tableDataFile
        assert(overwrite(tempFile, srcFile), this.die("File overwrite failure"))

        // update the table's configuration
        val cfg = newDevice.ns.getConfig
        host.ns.writeConfig(alterations.collectFirst {
          case SetLabel(description) => cfg.copy(description = Some(description.pullString._3))
        } || cfg)

        // delete the temporary file
        ResourceManager.close(newDevice.ns)
        ResourceManager.close(host.ns)
        (scope, cost, cost)
    }
  }

  override def toSQL: String = s"alter table ${ref.toSQL} ${alterations map (_.toSQL) mkString " "}"

  private def overwrite(src: File, dest: File): Boolean = {
    import com.lollypop.util.ResourceHelper.AutoClose
    val buf = new Array[Byte](65536)
    new FileInputStream(src) use { in =>
      new FileOutputStream(dest) use { out =>
        val count = in.read(buf)
        out.write(buf, 0, count)
      }
    }
    src.length() == dest.length()
  }

}

/**
 * Alter Table Companion
 */
object AlterTable extends ModifiableParser {
  val templateCard: String =
    """|alter table %L:name %O {{
       |?add +?column +?%P:add_col
       |?append +?column +?%P:append_col
       |?drop +?column +?%a:drop_col
       |?prepend +?column +?%P:prepend_col
       |?rename +?column +?%a:old_name +?to +?%a:new_name
       |?label +?%a:description
       |}}
       |""".stripMargin

  /**
   * Creates a new alter table operation
   * @param ref        the [[DatabaseObjectRef table reference]]
   * @param alteration the [[Alteration alteration]]
   * @return a new [[AlterTable alter table operation]]
   */
  def apply(ref: DatabaseObjectRef, alteration: Alteration) = new AlterTable(ref, alterations = Seq(alteration))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[AlterTable] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, templateCard)
      val alterations: List[Alteration] =
        params.parameters.get("add_col").map(columns => AddColumn(columns.onlyOne(ts).toColumn)).toList :::
          params.parameters.get("append_col").map(columns => AppendColumn(columns.onlyOne(ts).toColumn)).toList :::
          params.atoms.get("description").map(SetLabel).toList :::
          params.atoms.get("drop_col").map { case Atom(name) => DropColumn(name) }.toList :::
          params.parameters.get("prepend_col").map(columns => PrependColumn(columns.onlyOne(ts).toColumn)).toList ::: (
          for {
            Atom(oldName) <- params.atoms.get("old_name")
            Atom(newName) <- params.atoms.get("new_name")
          } yield RenameColumn(oldName, newName)).toList
      if (alterations.isEmpty) ts.dieExpectedAlteration()
      Some(AlterTable(ref = params.locations("name"), alterations = alterations))
    } else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "alter",
    category = CATEGORY_DATAFRAMES_INFRA,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Modifies the structure of a table",
    example =
      """|namespace "temp.examples"
         |drop if exists StockQuotes
         |create table StockQuotes(symbol: String(5), exchange: String(9), lastSale: Double) containing (
         ||----------------------------------------------------------|
         || exchange  | symbol | lastSale | lastSaleTime             |
         ||----------------------------------------------------------|
         || OTCBB     | YSZUY  |   0.2355 | 2023-10-19T23:25:32.886Z |
         || NASDAQ    | DMZH   | 183.1636 | 2023-10-19T23:26:03.509Z |
         || OTCBB     | VV     |          |                          |
         || NYSE      | TGPNF  |  51.6171 | 2023-10-19T23:25:32.166Z |
         || OTHER_OTC | RIZA   |   0.2766 | 2023-10-19T23:25:42.020Z |
         || NASDAQ    | JXMLB  |  91.6028 | 2023-10-19T23:26:08.951Z |
         ||----------------------------------------------------------|
         |)
         |alter table StockQuotes
         |  prepend column saleDate: DateTime = DateTime()
         |  rename column symbol to ticker
         |  label 'Stock quotes staging table'
         |ns('StockQuotes')
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "alter"

  /**
   * Represents a table alteration
   */
  sealed trait Alteration extends Instruction

  /**
   * Represents an alteration to add a column
   * @param column the [[Column]] to add the next position in the table
   */
  case class AddColumn(column: Column) extends Alteration {
    override def toSQL: String = s"add column ${column.toSQL}"
  }

  /**
   * Represents an alteration to append a column
   * @param column the [[Column]] to append the end of the table
   */
  case class AppendColumn(column: Column) extends Alteration {
    override def toSQL: String = s"append column ${column.toSQL}"
  }

  /**
   * Represents an alteration to remove a column
   * @param columnName the name of the column to remove from the table
   */
  case class DropColumn(columnName: String) extends Alteration {
    override def toSQL: String = s"drop column $columnName"
  }

  /**
   * Represents an alteration to prepend a column
   * @param column the [[Column]] to prepend to the first position in the table
   */
  case class PrependColumn(column: Column) extends Alteration {
    override def toSQL: String = s"prepend column ${column.toSQL}"
  }

  /**
   * Represents an alteration to rename a column
   * @param oldName the current column name
   * @param newName the new/replacement column name
   */
  case class RenameColumn(oldName: String, newName: String) extends Alteration {
    override def toSQL: String = s"rename column $oldName to $newName"
  }

  /**
   * Represents an alteration to set the table description
   * @param description the [[Atom description]] to set
   */
  case class SetLabel(description: Atom) extends Alteration {
    override def toSQL: String = s"label ${description.toSQL}"
  }

  /**
   * Item Sequence Utilities
   * @param items the collection of items
   * @tparam A the item type
   */
  final implicit class ItemSeqUtilities[A](val items: Seq[A]) extends AnyVal {
    @inline
    def onlyOne(ts: TokenStream): A = items.toList match {
      case value :: Nil => value
      case _ => ts.dieMultipleColumnsNotSupported()
    }
  }

}