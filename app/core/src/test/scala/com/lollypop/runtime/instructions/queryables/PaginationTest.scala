package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.dieIllegalType
import com.lollypop.language.models.@@
import com.lollypop.runtime.devices.PaginationSupport
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class PaginationTest extends AnyFunSpec with VerificationTools {

  describe(classOf[Pagination].getSimpleName) {

    it("should return the first 5 rows") {
      val (_, _, rc) = LollypopVM.searchSQL(Scope(),
        """|stocks =
           ||----------------------------------------------------------|
           || exchange  | symbol | lastSale | lastSaleTime             |
           ||----------------------------------------------------------|
           || OTCBB     | RPYM   |   0.4932 | 2023-10-02T01:57:31.086Z |
           || OTCBB     | EGBQ   |   0.6747 | 2023-10-02T01:57:09.991Z |
           || OTHER_OTC | PEMCQ  |   0.6176 | 2023-10-02T01:57:23.684Z |
           || NASDAQ    | IPHBY  | 113.9129 | 2023-10-02T01:57:01.837Z |
           || NASDAQ    | HLOQW  | 159.1307 | 2023-10-02T01:57:50.139Z |
           || NYSE      | WQN    | 177.4067 | 2023-10-02T01:57:17.371Z |
           || NASDAQ    | JONV   | 139.6465 | 2023-10-02T01:57:55.758Z |
           || NASDAQ    | KKLPE  | 135.2768 | 2023-10-02T01:57:07.520Z |
           || AMEX      | KHGRO  | 163.3631 | 2023-10-02T01:57:21.286Z |
           || NASDAQ    | GSCF   |  75.8721 | 2023-10-02T01:57:21.640Z |
           || NASDAQ    | ZEP    |   91.009 | 2023-10-02T01:57:03.740Z |
           || OTHER_OTC | KMUEH  |   0.2605 | 2023-10-02T01:57:03.702Z |
           || OTCBB     | WLXIM  |   0.6886 | 2023-10-02T01:57:45.739Z |
           || NASDAQ    | OVTS   | 153.5991 | 2023-10-02T01:57:23.061Z |
           || OTCBB     | YGIVQ  |   0.8364 | 2023-10-02T01:57:38.882Z |
           ||----------------------------------------------------------|
           |stocksP = pagination(select symbol, lastSale, exchange from stocks)
           |stocksP.first(5)
           |""".stripMargin)
      assert(rc.toMapGraph == List(
        Map("symbol" -> "RPYM", "lastSale" -> 0.4932, "exchange" -> "OTCBB"),
        Map("symbol" -> "EGBQ", "lastSale" -> 0.6747, "exchange" -> "OTCBB"),
        Map("symbol" -> "PEMCQ", "lastSale" -> 0.6176, "exchange" -> "OTHER_OTC"),
        Map("symbol" -> "IPHBY", "lastSale" -> 113.9129, "exchange" -> "NASDAQ"),
        Map("symbol" -> "HLOQW", "lastSale" -> 159.1307, "exchange" -> "NASDAQ")
      ))
    }

    it("should return the next 5 rows") {
      val (_, _, rc) = LollypopVM.searchSQL(Scope(),
        """|stocks =
           ||----------------------------------------------------------|
           || exchange  | symbol | lastSale | lastSaleTime             |
           ||----------------------------------------------------------|
           || OTCBB     | RPYM   |   0.4932 | 2023-10-02T01:57:31.086Z |
           || OTCBB     | EGBQ   |   0.6747 | 2023-10-02T01:57:09.991Z |
           || OTHER_OTC | PEMCQ  |   0.6176 | 2023-10-02T01:57:23.684Z |
           || NASDAQ    | IPHBY  | 113.9129 | 2023-10-02T01:57:01.837Z |
           || NASDAQ    | HLOQW  | 159.1307 | 2023-10-02T01:57:50.139Z |
           || NYSE      | WQN    | 177.4067 | 2023-10-02T01:57:17.371Z |
           || NASDAQ    | JONV   | 139.6465 | 2023-10-02T01:57:55.758Z |
           || NASDAQ    | KKLPE  | 135.2768 | 2023-10-02T01:57:07.520Z |
           || AMEX      | KHGRO  | 163.3631 | 2023-10-02T01:57:21.286Z |
           || NASDAQ    | GSCF   |  75.8721 | 2023-10-02T01:57:21.640Z |
           || NASDAQ    | ZEP    |   91.009 | 2023-10-02T01:57:03.740Z |
           || OTHER_OTC | KMUEH  |   0.2605 | 2023-10-02T01:57:03.702Z |
           || OTCBB     | WLXIM  |   0.6886 | 2023-10-02T01:57:45.739Z |
           || NASDAQ    | OVTS   | 153.5991 | 2023-10-02T01:57:23.061Z |
           || OTCBB     | YGIVQ  |   0.8364 | 2023-10-02T01:57:38.882Z |
           ||----------------------------------------------------------|
           |stocksP = pagination(select symbol, lastSale, exchange from stocks)
           |stocksP.first(5)
           |stocksP.next(5)
           |""".stripMargin)
      assert(rc.toMapGraph == List(
        Map("symbol" -> "WQN", "lastSale" -> 177.4067, "exchange" -> "NYSE"),
        Map("symbol" -> "JONV", "lastSale" -> 139.6465, "exchange" -> "NASDAQ"),
        Map("symbol" -> "KKLPE", "lastSale" -> 135.2768, "exchange" -> "NASDAQ"),
        Map("symbol" -> "KHGRO", "lastSale" -> 163.3631, "exchange" -> "AMEX"),
        Map("symbol" -> "GSCF", "lastSale" -> 75.8721, "exchange" -> "NASDAQ")
      ))
    }

    it("should return the previous 5 rows") {
      val (_, _, rc) = LollypopVM.searchSQL(Scope(),
        """|stocks =
           ||----------------------------------------------------------|
           || exchange  | symbol | lastSale | lastSaleTime             |
           ||----------------------------------------------------------|
           || OTCBB     | RPYM   |   0.4932 | 2023-10-02T01:57:31.086Z |
           || OTCBB     | EGBQ   |   0.6747 | 2023-10-02T01:57:09.991Z |
           || OTHER_OTC | PEMCQ  |   0.6176 | 2023-10-02T01:57:23.684Z |
           || NASDAQ    | IPHBY  | 113.9129 | 2023-10-02T01:57:01.837Z |
           || NASDAQ    | HLOQW  | 159.1307 | 2023-10-02T01:57:50.139Z |
           || NYSE      | WQN    | 177.4067 | 2023-10-02T01:57:17.371Z |
           || NASDAQ    | JONV   | 139.6465 | 2023-10-02T01:57:55.758Z |
           || NASDAQ    | KKLPE  | 135.2768 | 2023-10-02T01:57:07.520Z |
           || AMEX      | KHGRO  | 163.3631 | 2023-10-02T01:57:21.286Z |
           || NASDAQ    | GSCF   |  75.8721 | 2023-10-02T01:57:21.640Z |
           || NASDAQ    | ZEP    |   91.009 | 2023-10-02T01:57:03.740Z |
           || OTHER_OTC | KMUEH  |   0.2605 | 2023-10-02T01:57:03.702Z |
           || OTCBB     | WLXIM  |   0.6886 | 2023-10-02T01:57:45.739Z |
           || NASDAQ    | OVTS   | 153.5991 | 2023-10-02T01:57:23.061Z |
           || OTCBB     | YGIVQ  |   0.8364 | 2023-10-02T01:57:38.882Z |
           ||----------------------------------------------------------|
           |stocksP = pagination(select symbol, lastSale, exchange from stocks)
           |stocksP.first(10)
           |stocksP.previous(5)
           |""".stripMargin)
      assert(rc.toMapGraph == List(
        Map("symbol" -> "WQN", "lastSale" -> 177.4067, "exchange" -> "NYSE"),
        Map("symbol" -> "JONV", "lastSale" -> 139.6465, "exchange" -> "NASDAQ"),
        Map("symbol" -> "KKLPE", "lastSale" -> 135.2768, "exchange" -> "NASDAQ"),
        Map("symbol" -> "KHGRO", "lastSale" -> 163.3631, "exchange" -> "AMEX"),
        Map("symbol" -> "GSCF", "lastSale" -> 75.8721, "exchange" -> "NASDAQ")
      ))
    }

    it("should return the last 5 rows") {
      val (_, _, rc) = LollypopVM.searchSQL(Scope(),
        """|stocks =
           ||----------------------------------------------------------|
           || exchange  | symbol | lastSale | lastSaleTime             |
           ||----------------------------------------------------------|
           || OTCBB     | RPYM   |   0.4932 | 2023-10-02T01:57:31.086Z |
           || OTCBB     | EGBQ   |   0.6747 | 2023-10-02T01:57:09.991Z |
           || OTHER_OTC | PEMCQ  |   0.6176 | 2023-10-02T01:57:23.684Z |
           || NASDAQ    | IPHBY  | 113.9129 | 2023-10-02T01:57:01.837Z |
           || NASDAQ    | HLOQW  | 159.1307 | 2023-10-02T01:57:50.139Z |
           || NYSE      | WQN    | 177.4067 | 2023-10-02T01:57:17.371Z |
           || NASDAQ    | JONV   | 139.6465 | 2023-10-02T01:57:55.758Z |
           || NASDAQ    | KKLPE  | 135.2768 | 2023-10-02T01:57:07.520Z |
           || AMEX      | KHGRO  | 163.3631 | 2023-10-02T01:57:21.286Z |
           || NASDAQ    | GSCF   |  75.8721 | 2023-10-02T01:57:21.640Z |
           || NASDAQ    | ZEP    |   91.009 | 2023-10-02T01:57:03.740Z |
           || OTHER_OTC | KMUEH  |   0.2605 | 2023-10-02T01:57:03.702Z |
           || OTCBB     | WLXIM  |   0.6886 | 2023-10-02T01:57:45.739Z |
           || NASDAQ    | OVTS   | 153.5991 | 2023-10-02T01:57:23.061Z |
           || OTCBB     | YGIVQ  |   0.8364 | 2023-10-02T01:57:38.882Z |
           ||----------------------------------------------------------|
           |stocksP = pagination(select symbol, lastSale, exchange from stocks)
           |stocksP.last(5)
           |""".stripMargin)
      assert(rc.toMapGraph == List(
        Map("symbol" -> "ZEP", "lastSale" -> 91.009, "exchange" -> "NASDAQ"),
        Map("symbol" -> "KMUEH", "lastSale" -> 0.2605, "exchange" -> "OTHER_OTC"),
        Map("symbol" -> "WLXIM", "lastSale" -> 0.6886, "exchange" -> "OTCBB"),
        Map("symbol" -> "OVTS", "lastSale" -> 153.5991, "exchange" -> "NASDAQ"),
        Map("symbol" -> "YGIVQ", "lastSale" -> 0.8364, "exchange" -> "OTCBB")
      ))
    }

    it("should return the current row") {
      val (_, _, rc) = LollypopVM.searchSQL(Scope(),
        """|stocks =
           ||----------------------------------------------------------|
           || exchange  | symbol | lastSale | lastSaleTime             |
           ||----------------------------------------------------------|
           || OTCBB     | RPYM   |   0.4932 | 2023-10-02T01:57:31.086Z |
           || OTCBB     | EGBQ   |   0.6747 | 2023-10-02T01:57:09.991Z |
           || OTHER_OTC | PEMCQ  |   0.6176 | 2023-10-02T01:57:23.684Z |
           || NASDAQ    | IPHBY  | 113.9129 | 2023-10-02T01:57:01.837Z |
           || NASDAQ    | HLOQW  | 159.1307 | 2023-10-02T01:57:50.139Z |
           || NYSE      | WQN    | 177.4067 | 2023-10-02T01:57:17.371Z |
           || NASDAQ    | JONV   | 139.6465 | 2023-10-02T01:57:55.758Z |
           || NASDAQ    | KKLPE  | 135.2768 | 2023-10-02T01:57:07.520Z |
           || AMEX      | KHGRO  | 163.3631 | 2023-10-02T01:57:21.286Z |
           || NASDAQ    | GSCF   |  75.8721 | 2023-10-02T01:57:21.640Z |
           || NASDAQ    | ZEP    |   91.009 | 2023-10-02T01:57:03.740Z |
           || OTHER_OTC | KMUEH  |   0.2605 | 2023-10-02T01:57:03.702Z |
           || OTCBB     | WLXIM  |   0.6886 | 2023-10-02T01:57:45.739Z |
           || NASDAQ    | OVTS   | 153.5991 | 2023-10-02T01:57:23.061Z |
           || OTCBB     | YGIVQ  |   0.8364 | 2023-10-02T01:57:38.882Z |
           ||----------------------------------------------------------|
           |stocksP = pagination(select symbol, lastSale, exchange from stocks)
           |stocksP.current()
           |""".stripMargin)
      assert(rc.toMapGraph == List(Map("symbol" -> "RPYM", "lastSale" -> 0.4932, "exchange" -> "OTCBB")))
    }

    it("should indicate that more rows are available") {
      val (scope, _, _) = LollypopVM.searchSQL(Scope(),
        """|stocks =
           ||----------------------------------------------------------|
           || exchange  | symbol | lastSale | lastSaleTime             |
           ||----------------------------------------------------------|
           || OTCBB     | RPYM   |   0.4932 | 2023-10-02T01:57:31.086Z |
           || OTCBB     | EGBQ   |   0.6747 | 2023-10-02T01:57:09.991Z |
           || OTHER_OTC | PEMCQ  |   0.6176 | 2023-10-02T01:57:23.684Z |
           || NASDAQ    | IPHBY  | 113.9129 | 2023-10-02T01:57:01.837Z |
           || NASDAQ    | HLOQW  | 159.1307 | 2023-10-02T01:57:50.139Z |
           || NYSE      | WQN    | 177.4067 | 2023-10-02T01:57:17.371Z |
           || NASDAQ    | JONV   | 139.6465 | 2023-10-02T01:57:55.758Z |
           || NASDAQ    | KKLPE  | 135.2768 | 2023-10-02T01:57:07.520Z |
           || AMEX      | KHGRO  | 163.3631 | 2023-10-02T01:57:21.286Z |
           || NASDAQ    | GSCF   |  75.8721 | 2023-10-02T01:57:21.640Z |
           || NASDAQ    | ZEP    |   91.009 | 2023-10-02T01:57:03.740Z |
           || OTHER_OTC | KMUEH  |   0.2605 | 2023-10-02T01:57:03.702Z |
           || OTCBB     | WLXIM  |   0.6886 | 2023-10-02T01:57:45.739Z |
           || NASDAQ    | OVTS   | 153.5991 | 2023-10-02T01:57:23.061Z |
           || OTCBB     | YGIVQ  |   0.8364 | 2023-10-02T01:57:38.882Z |
           ||----------------------------------------------------------|
           |stocksP = pagination(select symbol, lastSale, exchange from stocks)
           |stocksP.first(14)
           |""".stripMargin)
      val stocksP = scope.getRowCollection(@@("stocksP")) match {
        case rc: PaginationSupport => rc
        case x => dieIllegalType(x)
      }
      assert(stocksP.hasNext)
    }

    it("should indicate that no more rows are available") {
      val (scope, _, _) = LollypopVM.searchSQL(Scope(),
        """|stocks =
           ||----------------------------------------------------------|
           || exchange  | symbol | lastSale | lastSaleTime             |
           ||----------------------------------------------------------|
           || OTCBB     | RPYM   |   0.4932 | 2023-10-02T01:57:31.086Z |
           || OTCBB     | EGBQ   |   0.6747 | 2023-10-02T01:57:09.991Z |
           || OTHER_OTC | PEMCQ  |   0.6176 | 2023-10-02T01:57:23.684Z |
           || NASDAQ    | IPHBY  | 113.9129 | 2023-10-02T01:57:01.837Z |
           || NASDAQ    | HLOQW  | 159.1307 | 2023-10-02T01:57:50.139Z |
           || NYSE      | WQN    | 177.4067 | 2023-10-02T01:57:17.371Z |
           || NASDAQ    | JONV   | 139.6465 | 2023-10-02T01:57:55.758Z |
           || NASDAQ    | KKLPE  | 135.2768 | 2023-10-02T01:57:07.520Z |
           || AMEX      | KHGRO  | 163.3631 | 2023-10-02T01:57:21.286Z |
           || NASDAQ    | GSCF   |  75.8721 | 2023-10-02T01:57:21.640Z |
           || NASDAQ    | ZEP    |   91.009 | 2023-10-02T01:57:03.740Z |
           || OTHER_OTC | KMUEH  |   0.2605 | 2023-10-02T01:57:03.702Z |
           || OTCBB     | WLXIM  |   0.6886 | 2023-10-02T01:57:45.739Z |
           || NASDAQ    | OVTS   | 153.5991 | 2023-10-02T01:57:23.061Z |
           || OTCBB     | YGIVQ  |   0.8364 | 2023-10-02T01:57:38.882Z |
           ||----------------------------------------------------------|
           |stocksP = pagination(select symbol, lastSale, exchange from stocks)
           |stocksP.last(1)
           |""".stripMargin)
      val stocksP = scope.getRowCollection(@@("stocksP")) match {
        case rc: PaginationSupport => rc
        case x => dieIllegalType(x)
      }
      assert(!stocksP.hasNext)
    }

    it("should indicate that no previous rows are available") {
      val (scope, _, _) = LollypopVM.searchSQL(Scope(),
        """|stocks =
           ||----------------------------------------------------------|
           || exchange  | symbol | lastSale | lastSaleTime             |
           ||----------------------------------------------------------|
           || OTCBB     | RPYM   |   0.4932 | 2023-10-02T01:57:31.086Z |
           || OTCBB     | EGBQ   |   0.6747 | 2023-10-02T01:57:09.991Z |
           || OTHER_OTC | PEMCQ  |   0.6176 | 2023-10-02T01:57:23.684Z |
           || NASDAQ    | IPHBY  | 113.9129 | 2023-10-02T01:57:01.837Z |
           || NASDAQ    | HLOQW  | 159.1307 | 2023-10-02T01:57:50.139Z |
           || NYSE      | WQN    | 177.4067 | 2023-10-02T01:57:17.371Z |
           || NASDAQ    | JONV   | 139.6465 | 2023-10-02T01:57:55.758Z |
           || NASDAQ    | KKLPE  | 135.2768 | 2023-10-02T01:57:07.520Z |
           || AMEX      | KHGRO  | 163.3631 | 2023-10-02T01:57:21.286Z |
           || NASDAQ    | GSCF   |  75.8721 | 2023-10-02T01:57:21.640Z |
           || NASDAQ    | ZEP    |   91.009 | 2023-10-02T01:57:03.740Z |
           || OTHER_OTC | KMUEH  |   0.2605 | 2023-10-02T01:57:03.702Z |
           || OTCBB     | WLXIM  |   0.6886 | 2023-10-02T01:57:45.739Z |
           || NASDAQ    | OVTS   | 153.5991 | 2023-10-02T01:57:23.061Z |
           || OTCBB     | YGIVQ  |   0.8364 | 2023-10-02T01:57:38.882Z |
           ||----------------------------------------------------------|
           |stocksP = pagination(select symbol, lastSale, exchange from stocks)
           |""".stripMargin)
      val stocksP = scope.getRowCollection(@@("stocksP")) match {
        case rc: PaginationSupport => rc
        case x => dieIllegalType(x)
      }
      assert(!stocksP.hasPrevious)
    }

    it("should indicate that previous rows are available") {
      val (scope, _, _) = LollypopVM.searchSQL(Scope(),
        """|stocks =
           ||----------------------------------------------------------|
           || exchange  | symbol | lastSale | lastSaleTime             |
           ||----------------------------------------------------------|
           || OTCBB     | RPYM   |   0.4932 | 2023-10-02T01:57:31.086Z |
           || OTCBB     | EGBQ   |   0.6747 | 2023-10-02T01:57:09.991Z |
           || OTHER_OTC | PEMCQ  |   0.6176 | 2023-10-02T01:57:23.684Z |
           || NASDAQ    | IPHBY  | 113.9129 | 2023-10-02T01:57:01.837Z |
           || NASDAQ    | HLOQW  | 159.1307 | 2023-10-02T01:57:50.139Z |
           || NYSE      | WQN    | 177.4067 | 2023-10-02T01:57:17.371Z |
           || NASDAQ    | JONV   | 139.6465 | 2023-10-02T01:57:55.758Z |
           || NASDAQ    | KKLPE  | 135.2768 | 2023-10-02T01:57:07.520Z |
           || AMEX      | KHGRO  | 163.3631 | 2023-10-02T01:57:21.286Z |
           || NASDAQ    | GSCF   |  75.8721 | 2023-10-02T01:57:21.640Z |
           || NASDAQ    | ZEP    |   91.009 | 2023-10-02T01:57:03.740Z |
           || OTHER_OTC | KMUEH  |   0.2605 | 2023-10-02T01:57:03.702Z |
           || OTCBB     | WLXIM  |   0.6886 | 2023-10-02T01:57:45.739Z |
           || NASDAQ    | OVTS   | 153.5991 | 2023-10-02T01:57:23.061Z |
           || OTCBB     | YGIVQ  |   0.8364 | 2023-10-02T01:57:38.882Z |
           ||----------------------------------------------------------|
           |stocksP = pagination(select symbol, lastSale, exchange from stocks)
           |stocksP.last(1)
           |""".stripMargin)
      val stocksP = scope.getRowCollection(@@("stocksP")) match {
        case rc: PaginationSupport => rc
        case x => dieIllegalType(x)
      }
      assert(stocksP.hasPrevious)
    }

  }

}
