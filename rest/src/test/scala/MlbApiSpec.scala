import munit._
import zio.http._

class MlbApiSpec extends munit.ZSuite {

  val app: App[Any] = MlbApi.static

  testZ("should be ok") {

    val req = Request.get(URL(Root / "text"))
    assertZ(app.runZIO(req).isSuccess)
  }

  testZ("should be ko") {
    val req = Request.get(URL(Root))
    assertZ(app.runZIO(req).isFailure)
  }
}
