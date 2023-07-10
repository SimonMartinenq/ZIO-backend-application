import zio.json._
import zio.jdbc._

import java.time.LocalDate


object GameDates {

  opaque type GameDate = LocalDate

  object GameDate {

    def apply(value: LocalDate): GameDate = value

    def unapply(gameDate: GameDate): LocalDate = gameDate
  }

  given CanEqual[GameDate, GameDate] = CanEqual.derived
  implicit val gameDateEncoder: JsonEncoder[GameDate] = JsonEncoder.localDate
  implicit val gameDateDecoder: JsonDecoder[GameDate] = JsonDecoder.localDate
}

object SeasonYears {

  opaque type SeasonYear <: Int = Int

  object SeasonYear {

    def apply(year: Int): SeasonYear = year

    def safe(value: Int): Option[SeasonYear] =
      Option.when(value >= 1876 && value <= LocalDate.now.getYear)(value)

    def unapply(seasonYear: SeasonYear): Int = seasonYear
  }

  given CanEqual[SeasonYear, SeasonYear] = CanEqual.derived
  implicit val seasonYearEncoder: JsonEncoder[SeasonYear] = JsonEncoder.int
  implicit val seasonYearDecoder: JsonDecoder[SeasonYear] = JsonDecoder.int
}

object HomeTeams {

  opaque type HomeTeam = String

  object HomeTeam {

    def apply(value: String): HomeTeam = value

    def unapply(homeTeam: HomeTeam): String = homeTeam
  }

  given CanEqual[HomeTeam, HomeTeam] = CanEqual.derived
  implicit val homeTeamEncoder: JsonEncoder[HomeTeam] = JsonEncoder.string
  implicit val homeTeamDecoder: JsonDecoder[HomeTeam] = JsonDecoder.string
}

object AwayTeams {

  opaque type AwayTeam = String

  object AwayTeam {

    def apply(value: String): AwayTeam = value

    def unapply(awayTeam: AwayTeam): String = awayTeam
  }

  given CanEqual[AwayTeam, AwayTeam] = CanEqual.derived
  implicit val awayTeamEncoder: JsonEncoder[AwayTeam] = JsonEncoder.string
  implicit val awayTeamDecoder: JsonDecoder[AwayTeam] = JsonDecoder.string
}

object HomeScores {

  opaque type HomeScore <: Int = Int

  object HomeScore {

    def apply(homeScore: Int): HomeScore = homeScore

    //Return the value of the score if the score is gratter than 0
    def safe(value: Int): Option[HomeScore] =
      Option.when(value >= 0)(value)

    def unapply(homeScore: HomeScore): Int = homeScore
  }

  given CanEqual[HomeScore, HomeScore] = CanEqual.derived
  implicit val homeScoreEncoder: JsonEncoder[HomeScore] = JsonEncoder.int
  implicit val homeScoreDecoder: JsonDecoder[HomeScore] = JsonDecoder.int
}

object AwayScores {

  opaque type AwayScore <: Int = Int

  object AwayScore {

    def apply(awayScore: Int): AwayScore = awayScore

    //Return the value of the score if the score is gratter than 0
    def safe(value: Int): Option[AwayScore] =
      Option.when(value >= 0)(value)

    def unapply(awayScore: AwayScore): Int = awayScore
  }

  given CanEqual[AwayScore, AwayScore] = CanEqual.derived
  implicit val awayScoreEncoder: JsonEncoder[AwayScore] = JsonEncoder.int
  implicit val awayScoreDecoder: JsonDecoder[AwayScore] = JsonDecoder.int
}

object EloProbHomes {

  opaque type EloProbHome <: Double = Double

  object EloProbHome {

    def apply(eloProbHome: Double): EloProbHome = eloProbHome

    def unapply(eloProbHome: EloProbHome): Double = eloProbHome
  }

  given CanEqual[EloProbHome, EloProbHome] = CanEqual.derived
  implicit val eloProbHomeEncoder: JsonEncoder[EloProbHome] = JsonEncoder.double
  implicit val eloProbHomeDecoder: JsonDecoder[EloProbHome] = JsonDecoder.double
}

object EloProbAways {

  opaque type EloProbAway <: Double = Double

  object EloProbAway {

    def apply(eloProbAway: Double): EloProbAway = eloProbAway

    def unapply(eloProbAway: EloProbAway): Double = eloProbAway
  }

  given CanEqual[EloProbAway, EloProbAway] = CanEqual.derived
  implicit val eloProbAwayEncoder: JsonEncoder[EloProbAway] = JsonEncoder.double
  implicit val eloProbAwayDecoder: JsonDecoder[EloProbAway] = JsonDecoder.double
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
  given CanEqual[Game, Game] = CanEqual.derived
  implicit val gameEncoder: JsonEncoder[Game] = DeriveJsonEncoder.gen[Game]
  implicit val gameDecoder: JsonDecoder[Game] = DeriveJsonDecoder.gen[Game]

  def unapply(game: Game): (GameDate, SeasonYear, HomeTeam, AwayTeam, Option[HomeScore], Option[AwayScore], EloProbHome, EloProbAway) = 
    (game.date, game.season, game.homeTeam, game.awayTeam, game.homeScore, game.awayScore, game.eloProbHome, game.eloProbAway)
  
  type Row = (String, Int, String, String, Option[Int], Option[Int], Double, Double)

  extension (g:Game)
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
