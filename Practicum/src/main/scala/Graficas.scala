package utpl.edu

import com.github.tototoshi.csv.*
import org.nspl.*
import org.nspl.data.HistogramData
import org.nspl.saddle.*
import org.nspl.awtrenderer.*
import org.saddle.*
import cats.*
import cats.effect.*
import cats.effect.unsafe.implicits.global
import cats.implicits.*
import doobie.*
import doobie.implicits.*

import java.io.File

case class Equipo(nombre: String, goles: Int)
case class Jugador1(apellido: String, apodo:String, goles:Int)
case class Resultado(tipo:String,cantidad:Int)

object Graficas {

  @main
  def principal3() =

    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/practicum",
      user = "root",
      password = "1234",
      logHandler = None
    )

    // Leer Csv Partidos y Goles
    val pathDataFile: String = "Data\\dsPartidosYGoles8.csv"
    val reader: CSVReader = CSVReader.open(new File(pathDataFile))
    val contentFilePyG: List[Map[String, String]] = reader.allWithHeaders()
    reader.close()

    // Leer Csv ALineaciones x Torneo
    val pathDataFile2: String = "Data\\dsAlineacionesXTorneo8.csv"
    val reader2: CSVReader = CSVReader.open(new File(pathDataFile2))
    val contentFileAxT: List[Map[String, String]] = reader2.allWithHeaders()
    reader.close()

    val datos: List[(String, Int)] = datosGrafica(contentFilePyG)
    chartBarPlot(datos)
    densityPlotShirts(contentFileAxT)
    ganadoresBarPlot(contentFilePyG)

    val listaInfoGolesEquipo: List[Equipo] = infoGolesEquipo("T-30","T-25")
      .transact(xa)
      .unsafeRunSync()
    boxPlotInfoGOles(listaInfoGolesEquipo.map(k => (k.nombre,k.goles.toDouble)))

    val goleadores: List[Jugador1] = Goleadores(10)
      .transact(xa)
      .unsafeRunSync()
    goleadoresBarPlot(goleadores.map(k => (k.apellido + " " + k.apodo,k.goles.toDouble)))

    val inforesultados: List[Resultado] = resultados()
      .transact(xa)
      .unsafeRunSync()
    resultadoBarPlot(inforesultados.map(k => (k.tipo,k.cantidad.toDouble)))


  // 1era Grafica dataset - autogoles por mundiales
  def datosGrafica(data: List[Map[String, String]]): List[(String, Int)] =
    val autogoles: List[(String, Int)] = data
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

    val bar = barplotHorizontal(series,
      xLabFontSize = Option(RelFontSize(1)),
      color = RedBlue(1, 12))(
      par
        .xLabelRotation(-77)
        .xNumTicks(0)
        .xlab("Mundiales")
        .ylab("Autogoles")
        .main("Autogoles por Mundial")

    )
    pngToFile(new File("graficos\\autogoles.png"), bar.build, 1000)

  // 2da Grafica dataset - Camisetas que usan los arqueros
  def densityPlotShirts(data: List[Map[String, String]]):Unit =
    val listaNroShirt: List[Double] = data
      .filter( row => row("squads_position_name") == "goal keeper")
      .map( row => row("squads_shirt_number").toDouble)

    val densityDefenderShirtNumber = xyplot(density(listaNroShirt.toVec.toSeq)-> line())(
      // Parametros del grafico
      par
        .xlab("Shirt number") // Nombre del eje x
        .ylab("freq.") // Nombre del eje y
        .main("Goal Keeper shirt number") // Nombre dle gráfico
    )
    pngToFile(new File("graficos\\shirts.png"), densityDefenderShirtNumber.build, 1000)

  // 3era Gráfica dataset - Ganador de los mundiales y cuántos mundiales tienen
  def ganadoresBarPlot(data: List[Map[String, String]]) =
    val ganadores: List[(String, Double)] = data
      .map(k => (k("matches_tournament_id"), k("tournaments_winner")))
      .distinct
      .groupBy(_._2)
      .map(k => (k._1, k._2.size.toDouble))
      .toList

    val indices = Index(ganadores.map(value => value._1).toArray)
    val values = Vec(ganadores.map(value => value._2).toArray)

    val series = Series(indices, values)

    val bar = barplotHorizontal(series,
      xLabFontSize = Option(RelFontSize(1)),
      color = RedBlue(1, 6))(
      par
        .xLabelRotation(-77)
        .xNumTicks(0)
        .xlab("Ganadores")
        .ylab("Nro Mundiales Ganados")
        .main("Ganadores de los Mundiales")

    )
    pngToFile(new File("graficos\\ganadores.png"), bar.build, 1000)

  // 1er Gráfica BD - Información de goles de dos equipos
  def infoGolesEquipo(id1:String, id2: String): ConnectionIO[List[Equipo]] =
    sql"SELECT c.name , COUNT(g.goalId) FROM goals g INNER JOIN teams t ON g.teamId = t.teamId INNER JOIN countries c ON c.countryId = t.nameCountryId WHERE t.teamId = $id1|| t.teamId = $id2 GROUP BY g.matchId, g.teamId"
      .query[Equipo]
      .to[List]

  def boxPlotInfoGOles(lista: List[(String,Double)]) =

    val boxpl2 = boxplotFromLabels(lista,Color.blue)(
      par
        .xLabelRotation(-77)
        .xNumTicks(0)
        .xlab("Equipos")
        .ylab("Goles")
        .main("InfoGoles")

    )
    pngToFile(new File("graficos\\infoGoles.png"), boxpl2.build, 2000)

  // 2da Gráfica BD - ¿Jugadores que han marcado más de 10 goles en mundiales?
  def Goleadores(goles: Int): ConnectionIO[List[Jugador1]] =
    sql"SELECT p.familyName, p.givenName, COUNT(g.goalId) FROM goals g INNER JOIN players p ON g.playerId = p.playerId GROUP BY p.playerId HAVING COUNT(g.goalId) > $goles ORDER BY 3;"
      .query[Jugador1]
      .to[List]

  def goleadoresBarPlot(lista: List[(String, Double)]) =

    val indices: Index[String] = Index(lista.map(value => value._1).toArray)
    val values: Vec[Double] = Vec(lista.map(value => value._2.toDouble).toArray)

    val series = Series(indices, values)

    val bar = barplotHorizontal(series,
      xLabFontSize = Option(RelFontSize(1)),
      color = RedBlue(10, 18))(
      par
        .xLabelRotation(-77)
        .xNumTicks(0)
        .xlab("Jugadores")
        .ylab("Goles")
        .main("Goleadores")

    )
    pngToFile(new File("graficos\\goleadores.png"), bar.build, 1000)

  // 3era Gráfica BD - ¿Cuántos partidos han terminado en empate,local como ganador, visitante como ganador?
  def resultados(): ConnectionIO[List[Resultado]] =
    sql"SELECT m.result, COUNT(m.result) FROM matches m GROUP BY m.result"
      .query[Resultado]
      .to[List]

  def resultadoBarPlot(lista: List[(String, Double)]) =
    val indices: Index[String] = Index(lista.map(value => value._1).toArray)
    val values: Vec[Double] = Vec(lista.map(value => value._2.toDouble).toArray)

    val series = Series(indices, values)

    val bar = barplotHorizontal(series,
      xLabFontSize = Option(RelFontSize(1)),
      color = RedBlue(1, 800))(
      par
        .xLabelRotation(-77)
        .xNumTicks(0)
        .xlab("Resultado")
        .ylab("Cantidad de Partidos")
        .main("Resultado de los Partidos")

    )
    pngToFile(new File("graficos\\resultados.png"), bar.build, 1000)

}
