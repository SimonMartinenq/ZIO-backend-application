Launch the projet : 
- Go into the terminal and enter the following commands : 
    - sbt
    - project rest
    - compile
    - ~reStart
- Go into an other terminal and enter the following command : 
    - curl "http://localhost:8080/games/count" 
    - The output shloud be : "223388 game(s) in historical data"

DataStructure.scala defines our data structure for our API. 
In this file, several opaque types are defined to represent specific entities related to Games, such as match dates, season years, home teams, away teams, home scores, away scores, Elo probabilities for home and away teams.
An opaque type is an abstract type whose internal representation is hidden, meaning that its underlying value cannot be accessed directly, but only via methods defined specifically for that type. 
Next, this file contains the definition of a Game class that uses these opaque types to represent a match. It also contains JSON encoders and decoders for serializing/deserializing Game objects, as well as a JdbcDecoder for mapping rows in the database to Game instances.

Main.scala implements an API for historical baseball (MLB) game data. It uses the ZIO framework to handle asynchronous effects and dependencies. The API provides several endpoints to access the data, such as the list of games, the number of games, the last twenty games of a specific team, the prediction of the result of a game and the last game between two teams. Data is stored in an in-memory H2 database, and games are loaded from a CSV file when the application starts up. The API responds to HTTP requests by returning text or JSON responses, depending on the URL requested.

Example of request you can do to test our API : 
- curl "http://localhost:8080/text"
- curl "http://localhost:8080/json"
- curl "http://localhost:8080/init"
- curl "http://localhost:8080/games"
- curl "http://localhost:8080/games/count"
- curl "http://localhost:8080/games/ATL"
- curl "http://localhost:8080/game/predict/ATL/NYM"
- curl "http://localhost:8080/game/latest/ATL/NYM"