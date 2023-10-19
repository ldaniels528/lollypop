package com.qwery.runtime.devices

import com.qwery.runtime.{QweryVM, ROWID, ROWID_NAME}
import com.qwery.util.OptionHelper.OptionEnrichment

/**
 * Represents a Record Collection Zoo
 */
object RecordCollectionZoo {

  final implicit class ProductToRow[A <: Product](val product: A) extends AnyVal {
    def productToRow(implicit collection: RecordMetadataIO): Row = {
      Map(product.productElementNames.toSeq zip product.productIterator: _*).toRow
    }
  }

  final implicit class MapToRow(val mappings: QMap[String, Any]) extends AnyVal {

    /**
     * Converts the key-values into a row
     * @param collection the implicit [[RecordMetadataIO]]
     * @return the equivalent [[Row]]
     */
    def toRow(implicit collection: RecordMetadataIO): Row = toRow(rowID = rowID || collection.getLength)

    /**
     * Converts the key-values into a row
     * @param rowID     the unique row ID
     * @param structure the implicit [[RecordStructure]]
     * @return the equivalent [[Row]]
     */
    def toRow(rowID: ROWID)(implicit structure: RecordStructure): Row = toRow(rowID, structure.columns)

    /**
     * Converts the key-values into a row
     * @param columns the collection of [[TableColumn columns]]
     * @return the equivalent [[Row]]
     */
    def toRow(columns: Seq[TableColumn]): Row = toRow(rowID = 0L, columns)

    /**
     * Converts the key-values into a row
     * @param rowID   the unique row ID
     * @param columns the collection of [[TableColumn columns]]
     * @return the equivalent [[Row]]
     */
    def toRow(rowID: ROWID, columns: Seq[TableColumn]): Row = {
      Row(id = rowID, metadata = RowMetadata(), columns = columns, fields = columns map { column =>
        Field(name = column.name, metadata = FieldMetadata(column), value = mappings.get(column.name).flatMap(Option(_)) ??
          column.defaultValue.map(QweryVM.evaluatePure).flatMap(Option(_)))
      })
    }

    def rowID: Option[ROWID] = mappings.collectFirst { case (name, id: ROWID) if name == ROWID_NAME => id }

  }

}
