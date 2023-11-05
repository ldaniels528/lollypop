package com.lollypop.database.server

import com.lollypop.die
import com.lollypop.runtime.datatypes.{BooleanType, Float64Type, StringType}
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.instructions.expressions.GraphResult
import org.jfree.chart.plot.{PlotOrientation, RingPlot}
import org.jfree.chart.util.TableOrder
import org.jfree.chart.{ChartFactory, ChartUtils, JFreeChart}
import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.data.general.DefaultPieDataset
import org.jfree.data.statistics.HistogramDataset
import org.jfree.data.xy.{DefaultXYZDataset, XYSeries, XYSeriesCollection}

import java.io.File

/**
 * Lollypop Chart Generator
 */
object LollypopChartGenerator {

  def generate(d: GraphResult): JFreeChart = {
    parseOptions(d) map {
      case DrawOp("area", title, orientation, legend, tooltips, urls) =>
        val (labelX, labelY) = get2P(d)
        val dataset = d.data.toCategoryDataset(d)
        ChartFactory.createAreaChart(title, labelX, labelY, dataset, orientation, legend, tooltips, urls)
      case DrawOp("bar", title, orientation, legend, tooltips, urls) =>
        val (labelX, labelY) = get2P(d)
        val dataset = d.data.toCategoryDataset(d)
        ChartFactory.createBarChart(title, labelX, labelY, dataset, orientation, legend, tooltips, urls)
      case DrawOp("polar", title, _, legend, tooltips, urls) =>
        val (labelX, labelY) = get2P(d)
        val dataset = d.data.toXYSeries(d)
        ChartFactory.createPolarChart(title, dataset, legend, tooltips, urls)
      case DrawOp("bubble", title, orientation, legend, tooltips, urls) =>
        val (labelX, labelY) = get2P(d)
        val dataset = new DefaultXYZDataset()
        val data = Array(Array(1.0, 2.0, 3.0), Array(2.0, 3.0, 4.0), Array(10.0, 20.0, 30.0))
        dataset.addSeries("Series 1", data)
        val bubbleChart = ChartFactory.createBubbleChart(title, labelX, labelY, dataset, orientation, legend, tooltips, urls)
        //        val plot = bubbleChart.getXYPlot
        //        plot.setRenderer(new XYBubbleRenderer())
        bubbleChart
      case DrawOp("histogram", title, orientation, legend, tooltips, urls) =>
        val (labelX, labelY) = get2P(d)
        val dataset = d.data.toHistogramDataset(labelY)
        ChartFactory.createHistogram(title, labelX, labelY, dataset, orientation, legend, tooltips, urls)
      case DrawOp("line", title, orientation, legend, tooltips, urls) =>
        val (labelX, labelY) = get2P(d)
        val dataset = d.data.toCategoryDataset(d)
        ChartFactory.createLineChart(title, labelX, labelY, dataset, orientation, legend, tooltips, urls)
      case DrawOp("pie", title, _, legend, tooltips, urls) =>
        val dataset = d.data.toPieDataset(d)
        ChartFactory.createPieChart(title, dataset, legend, tooltips, urls)
      case DrawOp("pie3d", title, _, legend, tooltips, urls) =>
        val dataset = d.data.toPieDataset(d)
        ChartFactory.createPieChart3D(title, dataset, legend, tooltips, urls)
      case DrawOp("ring", title, _, legend, tooltips, urls) =>
        val dataset = d.data.toPieDataset(d)
        val chart = ChartFactory.createRingChart(title, dataset, legend, tooltips, urls)
        chart.getPlot match {
          case plot: RingPlot => plot.setSectionDepth(0.30)
          case _ =>
        }
        chart
      case DrawOp("scatter", title, orientation, legend, tooltips, urls) =>
        val (_, labelX, labelY) = get3P(d)
        val dataset = d.data.toXYSeries(d)
        ChartFactory.createScatterPlot(title, labelX, labelY, dataset, orientation, legend, tooltips, urls)
      case DrawOp("xy", title, orientation, legend, tooltips, urls) =>
        val (_, labelX, labelY) = get3P(d)
        val dataset = d.data.toXYSeries(d)
        ChartFactory.createXYLineChart(title, labelX, labelY, dataset, orientation, legend, tooltips, urls)
      case op: DrawOp => die(s"Invalid chart type \"${op.shape}\"")
    } getOrElse die("Invalid chart options")
  }

  def generateFile(baseDirectory: File, d: GraphResult): File = {
    val chart = generate(d)
    val filename = (for {
      title <- d.options.get("title").map(StringType.convert)
      name = title.replace(' ', '_').filter(c => c.isLetterOrDigit || c == '_')
    } yield name).getOrElse(Integer.toString(d.hashCode(), 36))
    val imageFile = new File(baseDirectory, s"$filename.png")
    ChartUtils.saveChartAsPNG(imageFile, chart, 600, 400)
    imageFile
  }

  private def get2P(d: GraphResult): (String, String) = {
    d.data.columns.map(_.name) match {
      case Seq(label, value, _*) => (label, value)
      case x => die(s"At least two columns were expected: ${x.mkString(", ")}")
    }
  }

  private def get3P(d: GraphResult): (String, String, String) = {
    d.data.columns.map(_.name) match {
      case Seq(key, label, value, _*) => (key, label, value)
      case x => die(s"At least three columns were expected: ${x.mkString(", ")}")
    }
  }

  private def parseOptions(d: GraphResult): Option[DrawOp] = {
    val (labelName, valueName) = d.data.columns.map(_.name) match {
      case Seq(label, value, _*) => (label, value)
      case x => die(s"At least two columns were expected: ${x.mkString(", ")}")
    }
    val options = d.options
    for {
      shape <- options.get("shape").map(StringType.convert)
      title <- options.get("title").map(StringType.convert)
      orientation = options.get("orientation").map {
        case s: String if s equalsIgnoreCase "HORIZONTAL" => PlotOrientation.HORIZONTAL
        case s: String if s equalsIgnoreCase "VERTICAL" => PlotOrientation.VERTICAL
        case _ => PlotOrientation.VERTICAL
      } getOrElse PlotOrientation.VERTICAL
      legend = options.get("legend") match {
        case Some(value) => BooleanType.convert(value)
        case None => true
      }
      tooltips = options.get("tooltips") match {
        case Some(value) => BooleanType.convert(value)
        case None => true
      }
      urls = options.get("urls").exists(BooleanType.convert)
    } yield DrawOp(shape, title, orientation, legend, tooltips, urls)
  }

  private case class DrawOp(shape: String,
                            title: String,
                            orientation: PlotOrientation = PlotOrientation.VERTICAL,
                            legend: Boolean = true,
                            tooltips: Boolean = true,
                            urls: Boolean = false)

  final implicit class RowCollectionToCategoryDataset(val rc: RowCollection) extends AnyVal {

    def toCategoryDataset(d: GraphResult): DefaultCategoryDataset = {
      val (labelX, labelY) = get2P(d)
      val dataset = new DefaultCategoryDataset()
      for {
        row <- rc
        x <- row.get(labelX).map(StringType.convert)
        y <- row.get(labelY).map(Float64Type.convert)
      } dataset.addValue(y, x, labelY)
      dataset
    }

    def toHistogramDataset(valueKey: String, bins: Int = 5): HistogramDataset = {
      val dataset = new HistogramDataset()
      val values = for {
        row <- rc.toList
        v <- row.get(valueKey).map(Float64Type.convert).toList
      } yield v
      dataset.addSeries("Data Series", values.toArray, bins)
      dataset
    }

    def toPieDataset(d: GraphResult): DefaultPieDataset[String] = {
      val (labelX, labelY) = get2P(d)
      val dataset = new DefaultPieDataset[String]()
      for {
        row <- rc
        x <- row.get(labelX).map(StringType.convert)
        y <- row.get(labelY).map(Float64Type.convert)
      } dataset.setValue(x, y)
      dataset
    }

    def toXYSeries(d: GraphResult): XYSeriesCollection = {
      val (labelW, labelX, labelY) = get3P(d)
      rc.toList.flatMap { row =>
          for {
            w <- row.get(labelW).map(StringType.convert)
            x <- row.get(labelX).map(Float64Type.convert)
            y <- row.get(labelY).map(Float64Type.convert)
          } yield (w, x, y)
        }
        .groupBy(_._1)
        .map { case (name, values) =>
          val series = new XYSeries(name)
          values.foreach { case (_, x, y) => series.add(x, y) }
          series
        }
        .foldLeft(new XYSeriesCollection()) { case (coll, series) =>
          coll.addSeries(series)
          coll
        }
    }

  }

}
