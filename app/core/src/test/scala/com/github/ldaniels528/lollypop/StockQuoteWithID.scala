package com.github.ldaniels528.lollypop

import com.lollypop.runtime.{ColumnInfo, ROWID}

import scala.annotation.meta.field

case class StockQuoteWithID(@(ColumnInfo@field)(maxSize = 12) symbol: String,
                            @(ColumnInfo@field)(maxSize = 12) exchange: String,
                            lastSale: Double,
                            lastSaleTime: Long,
                            @(ColumnInfo@field)(typeDef = "RowNumber") _id: ROWID = 0)
