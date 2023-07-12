import zio._
import zio.jdbc._
import zio.http._
import com.github.tototoshi.csv._
import zio.stream.ZStream
import zio.schema._
import zio.schema.syntax._

import Game._
import GameDates.*
import SeasonYears.*
import HomeTeams.*
import AwayTeams.*
import HomeScores.*
import AwayScores.*
import EloProbHomes.*
import EloProbAways.*
import java.time.LocalDate


object MlbApi extends ZIOAppDefault {

  import DataService._
  import ApiService._

  // Definition of the static endpoint that returns a text or JSON response depending on the URL
  val static: App[Any] = Http.collect[Request] {
    case Method.GET -> Root / "text" => Response.text("Hello MLB Fans!")
    case Method.GET -> Root / "json" => Response.json("""{"greetings": "Hello MLB Fans!"}""")
  }.withDefaultErrorResponse

  // Define API endpoints using ZIO
  val endpoints: App[ZConnectionPool] = Http.collectZIO[Request] {

    // Endpoint for API initialization
    case Method.GET -> Root / "init" => 
      ZIO.succeed(Response.text("Our API is initialized").withStatus(Status.NotImplemented))

    // Endpoint to retrieve the list of the top 10 games
    case Method.GET -> Root / "games" =>
      for {
        list: List[Game] <- list
        res: Response = listResponse(list)
      } yield res
    
    // Endpoint to retrieve the number of games in historical data
    case Method.GET -> Root / "games" / "count" =>
      for {
        count: Option[Int] <- count
        res: Response = countResponse(count)
    } yield res
    
    // Endpoint to retrieve the last twenty games for a specific team
    case Method.GET -> Root / "games" / teamName  =>
      for {
        games: List[Game] <- lastTwentyGames(HomeTeam(teamName), AwayTeam(teamName))
        res: Response = latestTwentyGamesResponse(games)
      } yield res

    // Endpoint to predict the outcome of a match between two specific teams
    case Method.GET -> Root / "game" / "predict" / homeTeam / awayTeam =>
      for {
        predHome: Option[Double] <- getProbHome(HomeTeam(homeTeam), AwayTeam(awayTeam))
        predAway: Option[Double] <- getProbAway(HomeTeam(homeTeam), AwayTeam(awayTeam))
        res: Response = predictResponse(homeTeam, predHome, awayTeam, predAway)
      } yield res
    
    
    // Endpoint to recover the last play between two specific teams
    case Method.GET -> Root / "game" / "latest" / homeTeam / awayTeam =>
      for {
        game: Option[Game] <- latest(HomeTeam(homeTeam), AwayTeam(awayTeam))
        res: Response = latestGameResponse(game)
      } yield res

    // Default endpoint for unrecognized requests
    case _ =>
      ZIO.succeed(Response.text("Not Found").withStatus(Status.NotFound))
  }.withDefaultErrorResponse

  // Application entry point
  val app: ZIO[ZConnectionPool & Server, Throwable, Unit] = for {
    conn <- create
    // Path to CSV file (to be adapted to your environment)
    //WHEN RUN METHOD USE THIS PATH:
    //source <- ZIO.succeed(CSVReader.open(("rest\\src\\CsvFiles\\mlb_elo.csv")))
    //WHEN USING SBT USE THIS PATH:
    source <- ZIO.succeed(CSVReader.open(("src\\CsvFiles\\mlb_elo.csv")))
    //Stream = List()
    stream <- ZStream
      // Read CSV file as string sequence stream
      .fromIterator[Seq[String]](source.iterator)
      // Transform each string sequence into a Game object using opaque types
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
      // Group sets by batches of 1000 to insert them in the database
      .grouped(1000)
      // Convert groupings into lists and insert sets into the database
      .foreach(chunk => insertRows(chunk.toList))
    // Closing the CSV file
    _ <- ZIO.succeed(source.close())
    // Execute a select query in the database
    res <- select
    // Display query results in the console
    _ <- Console.printLine(res)
    // Launch HTTP server with defined endpoints
    _ <- Server.serve[ZConnectionPool](static ++ endpoints)
} yield ()

  override def run: ZIO[Any, Throwable, Unit] =
    // Run the application, providing the necessary dependencies
    app.provide(createZIOPoolConfig >>> connectionPool, Server.default)
}


object ApiService {

  import zio.json.EncoderOps
  import Game._

  // Generate the response for the list of the first 10 games
  def listResponse(list: List[Game]): Response = {
    Response.text("Entered").withStatus(Status.Ok)
    list match {
      case Nil => Response.text("List is empty").withStatus(Status.NotFound)
      case _ =>
        Response.text(s"${list.mkString("\n")}").withStatus(Status.Ok)
    }
  }

  // Generate response for number of sets in historical data
  def countResponse(count: Option[Int]): Response = {
    count match
      case Some(c) => Response.text(s"$c game(s) in historical data").withStatus(Status.Ok)
      case None => Response.text("No game in historical data").withStatus(Status.NotFound)
  }

  // Generate response for match prediction
  def predictResponse(homeTeam: String, predHome: Option[Double], awayTeam: String, predAway: Option[Double]): Response = {
    (predHome, predAway) match
      case (Some(d), Some(a)) => Response.text(s"Prediction for ${homeTeam} is $d \nPrediction for ${awayTeam} is $a").withStatus(Status.Ok)
      case (None, Some(a)) => Response.text(s"Prediction for ${homeTeam} not found \nPrediction for ${awayTeam} is $a").withStatus(Status.Ok)
      case (Some(d), None) => Response.text(s"Prediction for ${homeTeam} is $d \nPrediction for ${awayTeam} not found").withStatus(Status.Ok)
      case (None, None) => Response.text(s"Prediction for ${homeTeam} not found \nPrediction for ${awayTeam} not found").withStatus(Status.Ok)
  }

  // Generate response for the last game between two teams
  def latestGameResponse(game: Option[Game]): Response = {
    game match
      case Some(game) => Response.text(s"$game").withStatus(Status.Ok)
      case None => Response.text("No game found in historical data").withStatus(Status.NotFound)
  }

  
  // Generate response for the last twenty games of a specific team
  def latestTwentyGamesResponse(games: List[Game]): Response = {
  games match {
    case Nil => Response.text("No games found in historical data").withStatus(Status.NotFound)
    case _ =>
      Response.text(s"${games.mkString("\n")}").withStatus(Status.Ok)
    }
  }
}

object DataService {

  import Game._ 

  // Default connection pool configuration
  val createZIOPoolConfig: ULayer[ZConnectionPoolConfig] =
    ZLayer.succeed(ZConnectionPoolConfig.default)

  // Database connection properties
  val properties: Map[String, String] = Map(
    "user" -> "postgres",
    "password" -> "postgres"
  )

  // Creation of a database connection pool
  val connectionPool: ZLayer[ZConnectionPoolConfig, Throwable, ZConnectionPool] =
    ZConnectionPool.h2mem(
      database = "mlb",
      props = properties
    )

  // Create "games" table if it doesn't already exist
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

  // Insert a batch of games into the database
  def insertRows(games: List[Game]): ZIO[ZConnectionPool, Throwable, UpdateResult] = {
    val rows: List[Game.Row] = games.map(_.toRow)
    transaction {
      insert(
        sql"INSERT INTO games(date, season, homeTeam, awayTeam, homeScore, awayScore, eloProbHome, eloProbAway)".values[Game.Row](rows)
      )
    }
  }

  // Select a game from the database
  val select: ZIO[ZConnectionPool, Throwable, Option[Game]] = transaction {
   selectOne(
     sql"SELECT date, season, homeTeam, awayTeam, homeScore, awayScore, eloProbHome, eloProbAway FROM games"
        .as[Game]
    )
  }
  
  // Retrieve game list
  def list: ZIO[ZConnectionPool, Throwable, List[Game]] = {
    transaction{
      selectAll(
        sql"SELECT * FROM games LIMIT 10".as[Game]
      ).map(_.toList)
    }
  }

  // Retrieve the number of games from historical data
   val count: ZIO[ZConnectionPool, Throwable, Option[Int]] = transaction {
    selectOne(
      sql"SELECT COUNT(*) FROM games".as[Int]
    )
  }

  // Retrieve the last twenty games for a specific team
  def lastTwentyGames(homeTeam: HomeTeam, awayTeam: AwayTeam): ZIO[ZConnectionPool, Throwable, List[Game]] = {
    transaction {
      selectAll(
        sql"SELECT date, season, homeTeam, awayTeam, homeScore, awayScore, eloProbHome, eloProbAway FROM games WHERE homeTeam = ${HomeTeam.unapply(homeTeam)} OR awayTeam = ${AwayTeam.unapply(awayTeam)} ORDER BY date DESC LIMIT 20".as[Game]
      ).map(_.toList)
    }
  }

  // Retrieve the probability of home wins for a given match
  def getProbHome(homeTeam: HomeTeam, awayTeam: AwayTeam): ZIO[ZConnectionPool, Throwable, Option[Double]] = {
    transaction {
      selectOne(
        sql"SELECT eloProbHome FROM games WHERE homeTeam = ${HomeTeam.unapply(homeTeam)} AND awayTeam = ${AwayTeam.unapply(awayTeam)} ORDER BY date DESC LIMIT 1".as[Double]
      )
    }
  }

  // Retrieve the probability of an away win for a given match
  def getProbAway(homeTeam: HomeTeam, awayTeam: AwayTeam): ZIO[ZConnectionPool, Throwable, Option[Double]] = {
    transaction {
      selectOne(
        sql"SELECT eloProbAway FROM games WHERE homeTeam = ${HomeTeam.unapply(homeTeam)} AND awayTeam = ${AwayTeam.unapply(awayTeam)} ORDER BY date DESC LIMIT 1".as[Double]
      )
    }
  }

  // Retrieve the probability of an away win for a given match
  def latest(homeTeam: HomeTeam, awayTeam: AwayTeam): ZIO[ZConnectionPool, Throwable, Option[Game]] = {
    transaction {
      selectOne(
        sql"SELECT date, season, homeTeam, awayTeam, homeScore, awayScore, eloProbHome, eloProbAway FROM games WHERE homeTeam = ${HomeTeam.unapply(homeTeam)} AND awayTeam = ${AwayTeam.unapply(awayTeam)} ORDER BY date DESC LIMIT 1".as[Game]
      )
    }
  }
}

