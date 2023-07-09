import zio._
import zio.jdbc._
import zio.http._
import com.github.tototoshi.csv._
import zio.stream.ZStream
import zio.schema._
import zio.schema.syntax._

object MlbApi extends ZIOAppDefault {

  import DataService._

  val endpoints: App[ZConnectionPool] = Http.collectZIO[Request] {
       // Init si on fait pas dans le run
        case Method.GET -> Root / "init" => 
          ZIO.succeed(Response.text("Not Implemented").withStatus(Status.NotImplemented))

        //Faire une requete SQL
        case Method.GET -> Root / "games" => ???

        // case Method.GET -> Root / "predict" / "game" / gameId => ???
      }
      .withDefaultErrorResponse

  val app: ZIO[ZConnectionPool & Server, Throwable, Unit] = for {
    conn <- create
    source <- ZIO.succeed(CSVReader.open(("src/CsvFiles/mlb_elo_latest.csv")))
    //Stream = List()
    stream <- ZStream
      .fromIterator[Seq[String]](source.iterator)
      .map[Option[Game]] { values =>

        // Extraction des valeurs des colonnes
        val date = values(0)
        val season = values(1).toInt
        val homeTeam = values(4)
        val awayTeam = values(5)
        val homeScore = values(24).toIntOption
        val awayScore = values(25).toIntOption
        val eloProbHome = values(8).toDouble
        val eloProbAway = values(9).toDouble

        // Création d'un objet Game avec les valeurs extraites
        (homeScore, awayScore) match
          case (Some(homeScore), Some(awayScore)) =>
            Some(Game(date, season, homeTeam, awayTeam, homeScore, awayScore, eloProbHome, eloProbAway))
          case _ => None
      }
      .collectSome
      .grouped(1000) // Réduire le nombre de fois ou on fait insertRows
      .foreach(chunk => insertRows(chunk.toList))
    _ <- ZIO.succeed(source.close())
    res <- select
    _ <- Console.printLine(res)
    _ <- Server.serve(endpoints)
} yield ()

  override def run: ZIO[Any, Throwable, Unit] =
    app.provide(createZIOPoolConfig >>> connectionPool, Server.default)
}

object DataService {

  import Game._ 
  val createZIOPoolConfig: ULayer[ZConnectionPoolConfig] =
    ZLayer.succeed(ZConnectionPoolConfig.default)

  val properties: Map[String, String] = Map(
    "user" -> "postgres",
    "password" -> "postgres"
  )

  val connectionPool: ZLayer[ZConnectionPoolConfig, Throwable, ZConnectionPool] =
    ZConnectionPool.h2mem(
      database = "testdb",
      props = properties
    )

  val create: ZIO[ZConnectionPool, Throwable, Unit] = transaction {
    execute(
     sql"""CREATE TABLE IF NOT EXISTS games(
       date VARCHAR(255) NOT NULL,
        season INT NOT NULL, 
        homeTeam VARCHAR(255) NOT NULL,
        awayTeam VARCHAR(255) NOT NULL,
        homeScore INT NOT NULL,
        awayScore INT NOT NULL, 
        eloProbHome DOUBLE NOT NULL,
        eloProbAway DOUBLE NOT NULL
      )"""
    )
  }

  def insertRows(games: List[Game]): ZIO[ZConnectionPool, Throwable, UpdateResult] = {
   transaction {
     insert(
       sql"INSERT INTO games(date, season, homeTeam, awayTeam, homeScore, awayScore, eloProbHome, eloProbAway)"
         .values(games)
     )
    }
   }

   val select: ZIO[ZConnectionPool, Throwable, Option[Game]] = transaction {
   selectOne(
     sql"SELECT date, season, homeTeam, awayTeam, homeScore, awayScore, eloProbHome, eloProbAway FROM games"
        .as[Game]
    )
   }
}

//Definition of a Game
case class Game(
  date: String,
  season: Int, 
  homeTeam: String,
  awayTeam: String,
  homeScore: Int,
  awayScore: Int, 
  eloProbHome: Double,
  eloProbAway: Double
)

//Define a Schema to encode and decode a Game
object Game{
  import Schema.Field
  implicit val schema: Schema[Game] =
  Schema.CaseClass8[String, Int, String, String, Int, Int, Double, Double, Game](
    TypeId.parse(classOf[Game].getName),
    Field("date", Schema[String], get0 = _.date, set0 = (x, v) => x.copy(date = v)),
    Field("season", Schema[Int], get0 = _.season, set0 = (x, v) => x.copy(season = v)),
    Field("homeTeam", Schema[String], get0 = _.homeTeam, set0 = (x, v) => x.copy(homeTeam = v)),
    Field("awayTeam", Schema[String], get0 = _.awayTeam, set0 = (x, v) => x.copy(awayTeam = v)),
    Field("homeScore", Schema[Int], get0 = _.homeScore, set0 = (x, v) => x.copy(homeScore = v)),
    Field("awayScore", Schema[Int], get0 = _.awayScore, set0 = (x, v) => x.copy(awayScore = v)),
    Field("eloProbHome", Schema[Double], get0 = _.eloProbHome, set0 = (x, v) => x.copy(eloProbHome = v)),
    Field("eloProbAway", Schema[Double], get0 = _.eloProbAway, set0 = (x, v) => x.copy(eloProbAway = v)),
    Game.apply
  )
  implicit val jdbcDecoder: JdbcDecoder[Game] = JdbcDecoder.fromSchema
  implicit val jdbcEncoder: JdbcEncoder[Game] = JdbcEncoder.fromSchema
}