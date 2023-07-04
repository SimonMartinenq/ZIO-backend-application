package mlb

import zio._
import zio.jdbc._
import zio.http._
import com.github.tototoshi.csv._
import zio.stream.ZStream

object MlbApi extends ZIOAppDefault {

  case class Game(
    date: String,
    season: Int, 
    homeTeam: String,
    awayTeam: String,
    homeScore: Int,
    awayScore: Int, 
    eloProbHome: Int,
    eloProbAway: Int
  )

  val createZIOPoolConfig: ULayer[ZConnectionPoolConfig] =
    ZLayer.succeed(ZConnectionPoolConfig.default)

  val properties: Map[String, String] = Map(
    "user" -> "postgres",
    "password" -> "postgres"
  )

  val connectionPool : ZLayer[ZConnectionPoolConfig, Throwable, ZConnectionPool] =
    ZConnectionPool.h2mem(
      database = "testdb",
      props = properties
    )

  val create: ZIO[ZConnectionPool, Throwable, Unit] = transaction {
    execute(
      sql"""CREATE TABLE IF NOT EXISTS games( 
        date Date,
        season INT, 
        homeTeam VARCHAR(255) ,
        awayTeam VARCHAR(255),
        homeScore INT,
        awayScore INT, 
        eloProbHome INT,
        eloProbAway INT,
        PRIMARY KEY (`date`, `homeTeam`, `awayTeam`)
      )"""
    )
  }

  def insertRows(games : List[Game]): ZIO[ZConnectionPool, Throwable, UpdateResult] = transaction {
    insert(
      sql"INSERT INTO games (date, season, homeTeam, awayTeam, homeScore, awayScore, eloProbHome, eloProbAway) "
      .values(games.map { game =>
        val Game(date, season, homeTeam, awayTeam, homeScore, awayScore, eloProbHome, eloProbAway) = game
        (date, season, homeTeam, awayTeam, homeScore, awayScore, eloProbHome, eloProbAway)
      }) //décomposer la liste de Games unapply 
    )
  }


  val endpoints: App[Any] =
    Http
      //collectZIO
      .collect[Request] {
        //Init si on fait pas dans le run
        //case Method.GET -> Root / "init" => ???
        //Faire une requete SQL
        case Method.GET -> Root / "games" => ???
        // case Method.GET -> Root / "predict" / "game" / gameId => ???
      }
      .withDefaultErrorResponse

  val select: ZIO[ZConnectionPool, Throwable, Chunk[Game]] = transaction {
    selectOne(
      sql"SELECT date, season, homeTeam, awayTeam, homeScore, awayScore, eloProbHome, eloProbAway FROM games"
    ).query[(String, Int, String, String, Int, Int, Int, Int)].as[Game]
  }

  val app: ZIO[ZConnectionPool & Server, Throwable, Unit] = for {
    conn <- create
    source <- ZIO.succeed(CSVReader.open(getClass.getResource("/src/CsvFiles/mlb_elo.csv").getFile))
    //Stream = List()
    stream <- ZStream
      .fromIterator[Seq[String]](source.iterator)
      .map { values =>

        // Extraction des valeurs des colonnes
        val date = values(0)
        val season = values(1).toInt
        val homeTeam = values(2)
        val awayTeam = values(3)
        val homeScore = values(4).toInt
        val awayScore = values(5).toInt
        val eloProbHome = values(6).toInt
        val eloProbAway = values(7).toInt

        // Création d'un objet Game avec les valeurs extraites
        Game(date, season, homeTeam, awayTeam, homeScore, awayScore, eloProbHome, eloProbAway)
      }
      .grouped(10) //Réduire le nombre de fois ou on fait insertRows
      .foreach(chunk => insertRows(chunk.toList))
    _ <- ZIO.succeed(source.close())
    res <- select
    _ <- Server.serve(endpoints)
} yield res

  override def run: ZIO[Any, Throwable, Unit] =
    app.provide(createZIOPoolConfig >>> connectionPool, Server.default)
}