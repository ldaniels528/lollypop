package com.github.ldaniels528.qwery

import com.qwery.runtime.{ColumnInfo, ROWID}

import scala.annotation.meta.field

case class StockQuoteWithID(@(ColumnInfo@field)(maxSize = 12) symbol: String,
                            @(ColumnInfo@field)(maxSize = 12) exchange: String,
                            lastSale: Double,
                            lastSaleTime: Long,
                            @(ColumnInfo@field)(typeDef = "RowNumber") _id: ROWID = 0)
