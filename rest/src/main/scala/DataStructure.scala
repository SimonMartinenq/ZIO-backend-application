import zio.json._
import zio.jdbc._

import java.time.LocalDate

// Definition of an opaque type GameDate
object GameDates {

  opaque type GameDate = LocalDate

  object GameDate {

    // Apply method to create a GameDate from a LocalDate
    def apply(value: LocalDate): GameDate = value

    // Unapply method to extract the underlying LocalDate from a GameDate
    def unapply(gameDate: GameDate): LocalDate = gameDate
  }

  // Equality for GameDate
  given CanEqual[GameDate, GameDate] = CanEqual.derived
  // JSON encoder and decoder for GameDate
  implicit val gameDateEncoder: JsonEncoder[GameDate] = JsonEncoder.localDate
  implicit val gameDateDecoder: JsonDecoder[GameDate] = JsonDecoder.localDate
}

// Definition of an opaque type SeasonYear
object SeasonYears {

  opaque type SeasonYear <: Int = Int

  object SeasonYear {

    // Apply method to create a SeasonYear from an Int
    def apply(year: Int): SeasonYear = year

    // Safe method to create a SeasonYear from an Int within a certain range
    def safe(value: Int): Option[SeasonYear] =
      Option.when(value >= 1876 && value <= LocalDate.now.getYear)(value)

    // Unapply method to extract the underlying Int from a SeasonYear
    def unapply(seasonYear: SeasonYear): Int = seasonYear
  }

  // Equality for SeasonYear
  given CanEqual[SeasonYear, SeasonYear] = CanEqual.derived
  // JSON encoder and decoder for SeasonYear
  implicit val seasonYearEncoder: JsonEncoder[SeasonYear] = JsonEncoder.int
  implicit val seasonYearDecoder: JsonDecoder[SeasonYear] = JsonDecoder.int
}

// Definition of an opaque type HomeTeam
object HomeTeams {

  opaque type HomeTeam = String

  object HomeTeam {

    // Apply method to create a HomeTeam from a String
    def apply(value: String): HomeTeam = value

    // Unapply method to extract the underlying String from a HomeTeam
    def unapply(homeTeam: HomeTeam): String = homeTeam
  }

  // Equality for HomeTeam
  given CanEqual[HomeTeam, HomeTeam] = CanEqual.derived
  // JSON encoder and decoder for HomeTeam
  implicit val homeTeamEncoder: JsonEncoder[HomeTeam] = JsonEncoder.string
  implicit val homeTeamDecoder: JsonDecoder[HomeTeam] = JsonDecoder.string
}

// Definition of an opaque type AwayTeam
object AwayTeams {

  opaque type AwayTeam = String

  object AwayTeam {

    // Apply method to create an AwayTeam from a String
    def apply(value: String): AwayTeam = value

    // Unapply method to extract the underlying String from an AwayTeam
    def unapply(awayTeam: AwayTeam): String = awayTeam
  }

  // Equality for AwayTeam
  given CanEqual[AwayTeam, AwayTeam] = CanEqual.derived
  // JSON encoder and decoder for AwayTeam
  implicit val awayTeamEncoder: JsonEncoder[AwayTeam] = JsonEncoder.string
  implicit val awayTeamDecoder: JsonDecoder[AwayTeam] = JsonDecoder.string
}

// Definition of an opaque type HomeScore
object HomeScores {

  opaque type HomeScore <: Int = Int

  object HomeScore {

    // Apply method to create a HomeScore from an Int
    def apply(homeScore: Int): HomeScore = homeScore

    // Safe method to create a HomeScore from an Int if it is greater than or equal to 0
    def safe(value: Int): Option[HomeScore] =
      Option.when(value >= 0)(value)

    // Unapply method to extract the underlying Int from a HomeScore
    def unapply(homeScore: HomeScore): Int = homeScore
  }

  // Equality for HomeScore
  given CanEqual[HomeScore, HomeScore] = CanEqual.derived
  // JSON encoder and decoder for HomeScore
  implicit val homeScoreEncoder: JsonEncoder[HomeScore] = JsonEncoder.int
  implicit val homeScoreDencoder: JsonDecoder[HomeScore] = JsonDecoder.int
}

// Definition of an opaque type AwayScore
object AwayScores {

  opaque type AwayScore <: Int = Int

  object AwayScore {

    // Apply method to create an AwayScore from an Int
    def apply(awayScore: Int): AwayScore = awayScore

    // Safe method to create an AwayScore from an Int if it is greater than or equal to 0
    def safe(value: Int): Option[AwayScore] =
      Option.when(value >= 0)(value)

    // Unapply method to extract the underlying Int from an AwayScore
    def unapply(awayScore: AwayScore): Int = awayScore
  }

  // Equality for AwayScore
  given CanEqual[AwayScore, AwayScore] = CanEqual.derived
  // JSON encoder and decoder for AwayScore
  implicit val awayScoreEncoder: JsonEncoder[AwayScore] = JsonEncoder.int
  implicit val awayScoreDencoder: JsonDecoder[AwayScore] = JsonDecoder.int
}

// Definition of an opaque type EloProbHome
object EloProbHomes {

  opaque type EloProbHome <: Double = Double

  object EloProbHome {

    // Apply method to create an EloProbHome from a Double
    def apply(eloProbHome: Double): EloProbHome = eloProbHome

    // Unapply method to extract the underlying Double from an EloProbHome
    def unapply(eloProbHome: EloProbHome): Double = eloProbHome
  }

  // Equality for EloProbHome
  given CanEqual[EloProbHome, EloProbHome] = CanEqual.derived
  // JSON encoder and decoder for EloProbHome
  implicit val eloProbHomeEncoder: JsonEncoder[EloProbHome] = JsonEncoder.double
  implicit val eloProbHomeDencoder: JsonDecoder[EloProbHome] = JsonDecoder.double
}

// Definition of an opaque type EloProbAway
object EloProbAways {

  opaque type EloProbAway <: Double = Double

  object EloProbAway {

    // Apply method to create an EloProbAway from a Double
    def apply(eloProbAway: Double): EloProbAway = eloProbAway

    // Unapply method to extract the underlying Double from an EloProbAway
    def unapply(eloProbAway: EloProbAway): Double = eloProbAway
  }

  // Equality for EloProbAway
  given CanEqual[EloProbAway, EloProbAway] = CanEqual.derived
  // JSON encoder and decoder for EloProbAway
  implicit val eloProbAwayEncoder: JsonEncoder[EloProbAway] = JsonEncoder.double
  implicit val eloProbAwayDencoder: JsonDecoder[EloProbAway] = JsonDecoder.double
}


import GameDates.*
import SeasonYears.*
import HomeTeams.*
import AwayTeams.*
import HomeScores.*
import AwayScores.*
import EloProbHomes.*
import EloProbAways.*

//Definition of a Game
final case class Game(
  date: GameDate,
  season: SeasonYear, 
  homeTeam: HomeTeam,
  awayTeam: AwayTeam,
  homeScore: Option[HomeScore],
  awayScore: Option[AwayScore], 
  eloProbHome: EloProbHome,
  eloProbAway: EloProbAway
)

//Define a Schema to encode and decode a Game
object Game{
  // Equality for Game
  given CanEqual[Game, Game] = CanEqual.derived
  // JSON encoder and decoder for Game using zio-json's DeriveJsonEncoder and DeriveJsonDecoder
  implicit val gameEncoder: JsonEncoder[Game] = DeriveJsonEncoder.gen[Game]
  implicit val gameDecoder: JsonDecoder[Game] = DeriveJsonDecoder.gen[Game]

  // Unapply method to extract the fields of a Game
  def unapply(game: Game): (GameDate, SeasonYear, HomeTeam, AwayTeam, Option[HomeScore], Option[AwayScore], EloProbHome, EloProbAway) = 
    (game.date, game.season, game.homeTeam, game.awayTeam, game.homeScore, game.awayScore, game.eloProbHome, game.eloProbAway)
  
  // Type alias for a row representing a Game
  type Row = (String, Int, String, String, Option[Int], Option[Int], Double, Double)

  extension (g:Game)
    // Convert a Game to a row
    def toRow: Row =
      val (d, s, ht, at, hs, as, eh, sh) = Game.unapply(g)
      (
        GameDate.unapply(d).toString,
        SeasonYear.unapply(s),
        HomeTeam.unapply(ht),
        AwayTeam.unapply(at),
        hs.map(HomeScore.unapply),
        as.map(AwayScore.unapply),
        EloProbHome.unapply(eh),
        EloProbAway.unapply(sh)
      )

  // JdbcDecoder for Game, mapping a row to a Game instance
  implicit val jdbcDecoder: JdbcDecoder[Game] = JdbcDecoder[Row]().map[Game] { t =>
      val (date, season, homeTeam, awayTeam, maybeHomeScore, maybeAwayScore, eloProbHome, eloProbAway) = t
      Game(
        GameDate(LocalDate.parse(date)),
        SeasonYear(season),
        HomeTeam(homeTeam),
        AwayTeam(awayTeam),
        maybeHomeScore.map(HomeScore(_)),
        maybeAwayScore.map(AwayScore(_)),
        EloProbHome(eloProbHome),
        EloProbAway(eloProbAway),
      )
    }
    
}