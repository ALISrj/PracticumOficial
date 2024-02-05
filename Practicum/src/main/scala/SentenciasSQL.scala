package utpl.edu

import cats.*
import cats.effect.*
import cats.effect.unsafe.implicits.global
import cats.implicits.*
import doobie.*
import doobie.implicits.*

case class Host(id:Int,name:String)
case class Winner(id:Int, name:String, tournament:String)
case class Alineacion(torneo:String,team:String,position:String)
case class Estadio(nombre:String, ciudad:String, pais:String, capacidad:Int)
case class Jugador(apellido:String, apodo:String, fechaNacimiento:String)

object SentenciasSQL {

  @main
  def principal2() =

    //Conexion
    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/practicum",
      user = "root",
      password = "1234",
      logHandler = None
    )

    val resultHost: List[Host] = hostMundial("WC-2002")
      .transact(xa)
      .unsafeRunSync()
    resultHost.foreach(k => println(s"Id: ${k.id}, Nombre: ${k.name}"))
    println


    val winners: List[Winner] = ganadores()
      .transact(xa)
      .unsafeRunSync()
    winners.foreach(k => println(s"Nombre: ${k.name}\n\tId: ${k.id}\n\tMundial que ganó: ${k.tournament}"))
    println


    val alineacionesJugador: List[Alineacion] = alineacionesDJugador("Monti")
      .transact(xa)
      .unsafeRunSync()
    alineacionesJugador.foreach(k => println(s"Torneo: ${k.torneo}\tEquipo: ${k.team}\tPosición: ${k.position}"))
    println

    val estadios: List[Estadio] = estadiosXCapacidad(200000)
      .transact(xa)
      .unsafeRunSync()
    estadios.foreach(k => println(s"Nombre: ${k.nombre}\n\tCiudad: ${k.ciudad}\n\tPais: ${k.pais}\n\tCapacidad: ${k.capacidad}"))
    println

    val jugadores: List[Jugador] = fechaNacimiento("10-21")
      .transact(xa)
      .unsafeRunSync()
    jugadores.foreach(k => println(s"Jugador: ${k.apellido}\n\tApodo: ${k.apodo}\n\tFecha de Nacimiento: ${k.fechaNacimiento}"))

  def hostMundial(id:String): ConnectionIO[List[Host]] =
    sql"SELECT h.countryId, c.name FROM hostcountries h INNER JOIN countries c ON h.countryId = c.countryId WHERE tournamentId = $id"
      .query[Host]
      .to[List]

  def ganadores(): ConnectionIO[List[Winner]] =
    sql"SELECT c.countryId, c.name, t.name FROM countries c INNER JOIN tournaments t ON c.countryId = t.countryIdWinner"
      .query[Winner]
      .to[List]

  def alineacionesDJugador(nombre:String): ConnectionIO[List[Alineacion]] =
    sql"SELECT s.tournamentId, c.name, s.positionName FROM squads s INNER JOIN teams t ON s.teamId = t.teamId INNER JOIN countries c ON c.countryId = t.nameCountryId WHERE playerId = (SELECT playerId FROM players WHERE FamilyName = $nombre)"
      .query[Alineacion]
      .to[List]

  def estadiosXCapacidad(capacidad:Int): ConnectionIO[List[Estadio]] =
    sql"SELECT s.name, s.cityName, c.name, s.capacity FROM stadiums s INNER JOIN countries c ON s.countryId = c.countryId WHERE s.capacity = $capacidad"
      .query[Estadio]
      .to[List]

  def fechaNacimiento(fecha:String): ConnectionIO[List[Jugador]] =
    sql"SELECT p.familyName, p.givenName, p.birthdate FROM players p WHERE DATE_FORMAT(p.birthdate, '%m-%d') = $fecha"
      .query[Jugador]
      .to[List]
}
