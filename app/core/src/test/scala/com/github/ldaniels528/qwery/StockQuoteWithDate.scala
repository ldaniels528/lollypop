package com.github.ldaniels528.qwery

import com.qwery.runtime.ColumnInfo

import scala.annotation.meta.field

case class StockQuoteWithDate(@(ColumnInfo@field)(maxSize = 8) symbol: String,
                              @(ColumnInfo@field)(maxSize = 8) exchange: String,
                              lastSale: Double,
                              lastSaleTime: java.util.Date)
