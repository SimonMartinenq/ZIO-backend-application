import zio.*
import zio.jdbc.*
import zio.http.*
import com.github.tototoshi.csv.*
import zio.stream.ZStream
import zio.schema.*
import zio.schema.syntax.*
import Game.*
import GameDates.*
import SeasonYears.*
import HomeTeams.*
import AwayTeams.*
import HomeScores.*
import AwayScores.*
import DataService.list
import EloProbHomes.*
import EloProbAways.*

import java.time.LocalDate


object MlbApi extends ZIOAppDefault {

  import DataService._
  import ApiService._

  val static: App[Any] = Http.collect[Request] {
    case Method.GET -> Root / "text" => Response.text("Hello MLB Fans!")
    case Method.GET -> Root / "json" => Response.json("""{"greetings": "Hello MLB Fans!"}""")
  }.withDefaultErrorResponse

  val endpoints: App[ZConnectionPool] = Http.collectZIO[Request] {

    case Method.GET -> Root / "init" => 
      ZIO.succeed(Response.text("Not Implemented").withStatus(Status.NotImplemented))

    case Method.GET -> Root / "games" / "count" =>
      for {
        count: Option[Int] <- count
        res: Response = countResponse(count)
    } yield res

    case Method.GET -> Root / "games" =>
      for {
        list: List[Game] <- list
        res: Response = listResponse(list)
      } yield res

    case Method.GET -> Root / "game" / "predict" / homeTeam / awayTeam =>
      for {
        predHome: Option[Double] <- getProbHome(HomeTeam(homeTeam), AwayTeam(awayTeam))
        predAway: Option[Double] <- getProbAway(HomeTeam(homeTeam), AwayTeam(awayTeam))
        res: Response = predictResponse(homeTeam, predHome, awayTeam, predAway)
      } yield res
    
    case Method.GET -> Root / "game" / "latest" / homeTeam / awayTeam =>
      for {
        game: Option[Game] <- latest(HomeTeam(homeTeam), AwayTeam(awayTeam))
        res: Response = latestGameResponse(game)
      } yield res

    case _ =>
      ZIO.succeed(Response.text("Not Found").withStatus(Status.NotFound))
  }.withDefaultErrorResponse

  val app: ZIO[ZConnectionPool & Server, Throwable, Unit] = for {
    conn <- create
    //WHEN RUN METHOD USE THIS PATH:
    //source <- ZIO.succeed(CSVReader.open(("rest\\src\\CsvFiles\\mlb_elo.csv")))
    //WHEN USING SBT USE THIS PATH:
    source <- ZIO.succeed(CSVReader.open(("src\\CsvFiles\\mlb_elo.csv")))
    //Stream = List()
    stream <- ZStream
      .fromIterator[Seq[String]](source.iterator)
      .map { values =>
        val date = GameDates.GameDate(LocalDate.parse(values(0)))
        val season = SeasonYears.SeasonYear(values(1).toInt)
        val homeTeam = HomeTeams.HomeTeam(values(4))
        val awayTeam = AwayTeams.AwayTeam(values(5))
        val homeScore = HomeScores.HomeScore.safe(values(24).toIntOption.getOrElse(0))
        val awayScore = AwayScores.AwayScore.safe(values(25).toIntOption.getOrElse(0))
        val eloProbHome = EloProbHomes.EloProbHome(values(8).toDouble)
        val eloProbAway = EloProbAways.EloProbAway(values(9).toDouble)

        Game(date, season, homeTeam, awayTeam, homeScore, awayScore, eloProbHome, eloProbAway)
      }
      .grouped(1000)
      .foreach(chunk => insertRows(chunk.toList))
    _ <- ZIO.succeed(source.close())
    res <- select
    _ <- Console.printLine(res)
    _ <- Server.serve[ZConnectionPool](static ++ endpoints)
} yield ()

  override def run: ZIO[Any, Throwable, Unit] =
    app.provide(createZIOPoolConfig >>> connectionPool, Server.default)
}


object ApiService {

  import zio.json.EncoderOps
  import Game._

  def countResponse(count: Option[Int]): Response = {
    count match
      case Some(c) => Response.text(s"$c game(s) in historical data").withStatus(Status.Ok)
      case None => Response.text("No game in historical data").withStatus(Status.NotFound)
  }

  def listResponse(list: List[Game]): Response = {
    Response.text("Entered").withStatus(Status.Ok)
    list match {
      case Nil => Response.text("List is empty").withStatus(Status.NotFound)
      case _ =>
        Response.text(s"${list.mkString("\n")}").withStatus(Status.Ok)
    }
  }

  def predictResponse(homeTeam: String, predHome: Option[Double], awayTeam: String, predAway: Option[Double]): Response = {
    (predHome, predAway) match
      case (Some(d), Some(a)) => Response.text(s"Prediction for ${homeTeam} is $d \nPrediction for ${awayTeam} is $a").withStatus(Status.Ok)
      case (None, Some(a)) => Response.text(s"Prediction for ${homeTeam} not found \nPrediction for ${awayTeam} is $a").withStatus(Status.Ok)
      case (Some(d), None) => Response.text(s"Prediction for ${homeTeam} is $d \nPrediction for ${awayTeam} not found").withStatus(Status.Ok)
      case (None, None) => Response.text(s"Prediction for ${homeTeam} not found \nPrediction for ${awayTeam} not found").withStatus(Status.Ok)
  }

  def latestGameResponse(game: Option[Game]): Response = {
    println(game)
    game match
      case Some(game) => Response.text(s"$game").withStatus(Status.Ok)
      case None => Response.text("No game found in historical data").withStatus(Status.NotFound)
  }
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
      database = "mlb",
      props = properties
    )

  val create: ZIO[ZConnectionPool, Throwable, Unit] = transaction {
    execute(
     sql"""CREATE TABLE IF NOT EXISTS games(
        date DATE NOT NULL,
        season INT NOT NULL, 
        homeTeam VARCHAR(3) NOT NULL,
        awayTeam VARCHAR(3) NOT NULL,
        homeScore INT,
        awayScore INT,
        eloProbHome DOUBLE NOT NULL,
        eloProbAway DOUBLE NOT NULL
      )"""
    )
  }

  def insertRows(games: List[Game]): ZIO[ZConnectionPool, Throwable, UpdateResult] = {
    val rows: List[Game.Row] = games.map(_.toRow)
    transaction {
      insert(
        sql"INSERT INTO games(date, season, homeTeam, awayTeam, homeScore, awayScore, eloProbHome, eloProbAway)".values[Game.Row](rows)
      )
    }
  }

  val select: ZIO[ZConnectionPool, Throwable, Option[Game]] = transaction {
   selectOne(
     sql"SELECT date, season, homeTeam, awayTeam, homeScore, awayScore, eloProbHome, eloProbAway FROM games"
        .as[Game]
    )
  }

  val count: ZIO[ZConnectionPool, Throwable, Option[Int]] = transaction {
    selectOne(
      sql"SELECT COUNT(*) FROM games".as[Int]
    )
  }

  val list: ZIO[ZConnectionPool, Throwable, List[Game]] = transaction {
    selectAll(
      sql"SELECT * FROM games LIMIT 10".as[Game]
    ).map(_.toList)
  }
  def getProbHome(homeTeam: HomeTeam, awayTeam: AwayTeam): ZIO[ZConnectionPool, Throwable, Option[Double]] = {
    transaction {
      selectOne(
        sql"SELECT eloProbHome FROM games WHERE homeTeam = ${HomeTeam.unapply(homeTeam)} AND awayTeam = ${AwayTeam.unapply(awayTeam)} ORDER BY date DESC LIMIT 1".as[Double]
      )
    }
  }

  def getProbAway(homeTeam: HomeTeam, awayTeam: AwayTeam): ZIO[ZConnectionPool, Throwable, Option[Double]] = {
    transaction {
      selectOne(
        sql"SELECT eloProbAway FROM games WHERE homeTeam = ${HomeTeam.unapply(homeTeam)} AND awayTeam = ${AwayTeam.unapply(awayTeam)} ORDER BY date DESC LIMIT 1".as[Double]
      )
    }
  }

  def latest(homeTeam: HomeTeam, awayTeam: AwayTeam): ZIO[ZConnectionPool, Throwable, Option[Game]] = {
    transaction {
      selectOne(
        sql"SELECT date, season, homeTeam, awayTeam, homeScore, awayScore, eloProbHome, eloProbAway FROM games WHERE homeTeam = ${HomeTeam.unapply(homeTeam)} AND awayTeam = ${AwayTeam.unapply(awayTeam)} ORDER BY date DESC LIMIT 1".as[Game]
      )
    }
  }
}

