package utpl.edu

import com.github.tototoshi.csv.*

import java.io.File

object EstadisticasDescriptivas {

  @main
  def principal() =

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

    minMaxPromEstadios(contentFilePyG)
    modaGolesLocal(contentFilePyG)
    minMaxRegiones(contentFilePyG)
    drawFrecuencia(contentFilePyG)
    alineacions21(contentFileAxT)
    goalkepperForward(contentFileAxT)

  // ? Cuál es el estadio con menor capacidad, mayor capacidad y el promedio de capacidad entre los estadios?
  def minMaxPromEstadios(data: List[Map[String, String]]) =
    val estadios: List[(String, Int)] = data
      .map(k => (k("stadiums_stadium_name"),
        k("stadiums_stadium_capacity").toInt)
      )
      .distinct

    estadios.filter(_._2 == estadios.maxBy(_._2)._2).foreach(k => println(s"Estadio con mayor capacidad ${k._1}:${k._2}"))
    estadios.filter(_._2 == estadios.minBy(_._2)._2).foreach(k => println(s"Estadio con menor capacidad ${k._1}:${k._2}"))
    println(s"El promedio de capacidad en los estadios es ${estadios.map(_._2).sum / estadios.size.toDouble})")

  // ? Cuál es el número de goles más regular que un equipo local marca?
  def modaGolesLocal(data: List[Map[String, String]]) =
    val goles: (String, Int) = data
      .map(k => (k("matches_match_id"), k("matches_home_team_score")))
      .distinct // Lista de tuplas
      .map(_._2) // Lista de Strings
      .groupBy(k => k) // Mapa [String, List[String]]
      .map(k => (k._1, k._2.size)) // Mapa [String, Int]
      .maxBy(_._2) // Tupla (String, Int)

    println(s"La moda es ${goles._1} con ${goles._2} repeticiones")
    // La moda es 1 con 349 repeticiones

  // ? Cuál región ha mandado más paises al mundial y cual ha mandado menos paises?
  def minMaxRegiones(data: List[Map[String, String]]) =
    val regiones: Map[String, Int] = data
      .map(k => (k("matches_away_team_id"), k("away_region_name"))) // Lista de tuplas
      .distinct
      .groupBy(_._2) // Mapa [String, List[String]]
      .map(k => (k._1, k._2.size)) // Mapa [String, Int]

    println(s"La región con más equipos en el mundial es ${regiones.maxBy(_._2)._1} " +
      s"con ${regiones.maxBy(_._2)._2} equipos")
    println(s"La región con menos equipos en el mundial es ${regiones.minBy(_._2)._1} " +
      s"con ${regiones.minBy(_._2)._2} equipos")

    // La región con más equipos en el mundial es Europe con 35 equipos
    // La región con menos equipos en el mundial es Oceania con 2 equipos

  // ? Cuál es la cantidad de empates?
  def drawFrecuencia(data: List[Map[String, String]]) =
    val draw: Int = data
      .map(k => (k("matches_match_id"), k("matches_result")))
      .distinct
      .map(_._2) // Nos quedamos con los resultados
      .groupBy(k => k) // Agrupamos por si mismas
      .map(k => (k._1, k._2.size))("draw") // Accedo a la cantidad de empates

    println(s"Existen $draw empates") // Existen 210 empates

  // ¿ En cuántas alineaciones se ha usado el número 21?
  def alineacions21(data: List[Map[String, String]]) =
    val alineaciones: Int = data
      .map(k => (k("squads_player_id"), k("squads_tournament_id"), k("squads_shirt_number")))
      .count(_._3 == "21")

    println(s"Se ha usado en $alineaciones")
  // Se ha usado en 509 alineaciones

  // ¿ Cantidad de jugadores que han jugado como arqueros y  delanteros?
  def goalkepperForward(data: List[Map[String, String]]) =
    val jugadores: Int = data
      .map(k => (k("squads_player_id"), k("players_goal_keeper"), k("players_forward")))
      .distinct
      .count(k => k._2 == "1" && k._3 == "1")

    println(s"La cantudad de jugadores que han jugado como arqueras y delanteras es $jugadores")
    // 2 jugadores

}
