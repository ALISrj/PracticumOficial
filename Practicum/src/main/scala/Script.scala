package utpl.edu

import cats.*
import cats.effect.*
import cats.effect.unsafe.implicits.global
import cats.implicits.*
import com.github.tototoshi.csv.*
import doobie.*
import doobie.implicits.*

import java.io.File

//implicit object MyFormat extends DefaultCSVFormat {
//  override val delimiter = ';'
//}

object Script {

  @main
  def main() =

    //Conexion
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

    generateData2GoalsTable(contentFilePyG).foreach(insert => insert.run.transact(xa).unsafeRunSync())
    


  def generateData2CountriesTable(data: List[Map[String,String]]) =
    val insertFormat: String = s"INSERT INTO countries VALUES(%d,'%s');"
    val countries: List[String] = data
      .map(k => (k("IdAway").toInt,k("away_team_name"))) // Obtenemos los paises junto a su ID
      .distinct // Eliminamos los repetidos
      .sorted
      .map(tupla2 => insertFormat.format(tupla2._1,tupla2._2))

    countries.foreach(println)

  def generateData2TournamentsTable(data: List[Map[String,String]]) =
    val insertFormat: String = s"INSERT INTO tournaments VALUES('%s','%s',%d,%d,%d);"
    val tournaments: List[String] = data
      .map(
        k => (k("matches_tournament_id"),
          k("tournaments_tournament_name").replaceAll("'","\\\\'"),
          k("tournaments_year").toInt,
          k("countrywinnerid").toInt,
          k("tournaments_count_teams").toInt))
      .distinct // Eliminamos repetidos
      .sortBy(_._3) // Ordenamos por el anio
      .map(tupla5 => insertFormat.format(tupla5._1,tupla5._2,tupla5._3,tupla5._4,tupla5._5))

    tournaments.foreach(println)

  def generateData2StadiumsTable(data: List[Map[String,String]]) =
    val insertFormat: String = s"INSERT INTO stadiums VALUES('%s','%s','%s',%d,%d);"
    val stadiums:List[String] = data
      .map(
        k => (k("matches_stadium_id"),
          k("stadiums_stadium_name").replaceAll("'","\\\\'"),
          k("stadiums_city_name").replaceAll("'","\\\\'"),
          k("countryestadio").toInt,
          k("stadiums_stadium_capacity").toInt)
        )
      .distinct // Eliminamos repetidos
      .sortBy(_._1) // Ordenamos por el Id
      .map(tupla5 => insertFormat.format(tupla5._1,tupla5._2,tupla5._3,tupla5._4,tupla5._5))

      stadiums.foreach(println)

  def generateData2TeamsTable(data: List[Map[String,String]]) =
    val insertFormat: String = s"INSERT INTO teams VALUES('%s',%d,%d,%d,'%s');"
    val teams: List[String] = data
      .map(k => (k("matches_away_team_id"),
        k("IdAway").toInt,
        k("away_mens_team").toInt,
        k("away_womens_team").toInt,
        k("away_region_name"))
      )
      .distinct // ELiminamos repetidos
      .sortBy(_._1) // Ordenamos por ID
      .map(tupla5 => insertFormat.format(tupla5._1,tupla5._2,tupla5._3,tupla5._4,tupla5._5))

    teams.foreach(println)
    println(teams.size)

  def generateData2HostcountriesTable(data: List[Map[String,String]]) =
    val insertFormat: String = s"INSERT INTO hostcountries (countryId, tournamentId) VALUES(%d,'%s');"
    val hostCountries: List[String] = data
      .map( k => (k("hostcountryid"),k("matches_tournament_id")))
      .distinct
      .filterNot(_._2 == "WC-2002")
      .map(tupla2 => insertFormat.format(tupla2._1.toInt,tupla2._2))

    hostCountries.foreach(println)
    println("INSERT INTO hostcountries (countryId, tournamentId) VALUES(71,'WC-2002');")
    println("INSERT INTO hostcountries (countryId, tournamentId) VALUES(44,'WC-2002');")

  def generateData2SquadsTable(data: List[Map[String,String]]) =
    val squads = data
      .map(k => (k("squads_player_id"),
        k("squads_tournament_id"),
        k("squads_team_id"),
        k("squads_shirt_number").toInt,
        k("squads_position_name"))
      )
      .map(tupla5 => sql"INSERT INTO squads VALUES(${tupla5._1},${tupla5._2},${tupla5._3},${tupla5._4},${tupla5._5})".update)
    squads

  def generateData2PlayersTable(data: List[Map[String,String]]) =
    val players = data
      .map(k=> (k("squads_player_id"),
        k("players_family_name"),
        k("players_given_name"),
        fechas(k("players_birth_date")),
        k("players_female").toInt,
        k("players_goal_keeper").toInt,
        k("players_defender").toInt,
        k("players_midfielder").toInt,
        k("players_forward").toInt))
      .distinct
      .map(tupla9 =>  sql"INSERT INTO players VALUES(${tupla9._1},${tupla9._2},${tupla9._3},${tupla9._4},${tupla9._5},${tupla9._6},${tupla9._7},${tupla9._8},${tupla9._9})".update)
    players

  def fechas(cadena:String): Option[String] =
    if (cadena == "not available"){
      None
    } else {
      Some(cadena)
    }

  def generateData2MatchesTable(data: List[Map[String,String]]) =
    val matches = data
      .map(k => (k("matches_match_id"),
        fechas(k("matches_match_date")),
        k("matches_match_time"),
        k("matches_stage_name"),
        k("matches_home_team_score").toInt,
        k("matches_away_team_score").toInt,
        k("matches_extra_time").toInt,
        k("matches_penalty_shootout").toInt,
        k("matches_home_team_score_penalties").toInt,
        k("matches_away_team_score_penalties").toInt,
        k("matches_result"),
        k("matches_stadium_id"),
        k("matches_tournament_id"),
        k("matches_home_team_id"),
        k("matches_away_team_id"))
      ).distinct
      .map(tupla15 =>  sql"INSERT INTO matches VALUES(${tupla15._1},${tupla15._2},${tupla15._3},${tupla15._4},${tupla15._5},${tupla15._6},${tupla15._7},${tupla15._8},${tupla15._9},${tupla15._10},${tupla15._11},${tupla15._12},${tupla15._13},${tupla15._14},${tupla15._15})".update)

      matches

  def generateData2GoalsTable(data: List[Map[String,String]]) =
    val goals = data
      .map(k => (k("goals_goal_id"),
        k("goals_minute_label"),
        k("goals_minute_regulation"),
        k("goals_minute_stoppage"),
        k("goals_match_period"),
        k("goals_own_goal"),
        k("goals_penalty"),
        k("matches_match_id"),
        k("goals_player_id"),
        k("goals_player_team_id"),
        k("goals_team_id"),
        k("matches_tournament_id"))
      )
      .filterNot(_._1 == "NA")
      .map(tupla12 => sql"INSERT INTO goals VALUES(${tupla12._1},${tupla12._2},${tupla12._3.toInt},${tupla12._4.toInt},${tupla12._5},${tupla12._6.toInt},${tupla12._7.toInt},${tupla12._8},${tupla12._9},${tupla12._10},${tupla12._11},${tupla12._12})".update)

    goals



}
