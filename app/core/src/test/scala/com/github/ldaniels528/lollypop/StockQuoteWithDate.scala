package com.github.ldaniels528.lollypop

import com.lollypop.runtime.ColumnInfo

import scala.annotation.meta.field

case class StockQuoteWithDate(@(ColumnInfo@field)(maxSize = 8) symbol: String,
                              @(ColumnInfo@field)(maxSize = 8) exchange: String,
                              lastSale: Double,
                              lastSaleTime: java.util.Date)
