package utpl.edu

import com.github.tototoshi.csv.*

import org.nspl._
import org.nspl.data.HistogramData
import org.nspl.saddle._
import org.nspl.awtrenderer._
import org.saddle._


import java.io.File


object Graficas {

  @main
  def principal3() =

    // Leer Csv Partidos y Goles
    val pathDataFile: String = "Data\\dsPartidosYGoles8.csv"
    val reader: CSVReader = CSVReader.open(new File(pathDataFile))
    val contentFilePyG: List[Map[String, String]] = reader.allWithHeaders()
    reader.close()

    val datos: List[(String, Int)] = datosGrafica(contentFilePyG)
    chartBarPlot(datos)




  // 1era Grafica dataset
  def datosGrafica(data: List[Map[String, String]]): List[(String, Int)] =
    val autogoles = data
      .map(row => (
        row("tournaments_year"),
        row("goals_own_goal"),
      ))
      .filter(_._2 == "1")
      .groupBy(_._1)
      .map(k => (k._1, k._2.size))
      .toList
    autogoles

  def chartBarPlot(data: List[(String, Int)]): Unit =

    val data1: List[(String, Double)] = data
      .map(t2 => (t2._1, t2._2.toDouble))

    val indices = Index(data1.map(value => value._1).toArray)
    val values = Vec(data1.map(value => value._2).toArray)

    val series = Series(indices, values)

    val bar1 = barplotHorizontal(series,
      xLabFontSize = Option(RelFontSize(1)),
      color = RedBlue(1, 12))(
      par
        .xLabelRotation(-77)
        .xNumTicks(0)
        .xlab("Mundiales")
        .ylab("Autogoles")
        .main("Autogoles por Mundial")

    )
    pngToFile(new File("graficos\\autogoles.png"), bar1.build, 1000)

  // 2da Grafica dataset


}
