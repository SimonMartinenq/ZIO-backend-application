Launch the projet : 
    - Go into the terminal and enter the following commands : 
        - sbt
        - project rest
        - compile
        - ~reStart
    Go into an other terminal and enter the following command : 
        - curl "http://localhost:8080/games/count"

DataStructure.scala : 
This file defines our data structure for our API. 
In this file, several opaque types are defined to represent specific entities related to Games, such as match dates, season years, home teams, away teams, home scores, away scores, Elo probabilities for home and away teams.
An opaque type is an abstract type whose internal representation is hidden, meaning that its underlying value cannot be accessed directly, but only via methods defined specifically for that type. 
Next, this file contains the definition of a Game class that uses these opaque types to represent a match. It also contains JSON encoders and decoders for serializing/deserializing Game objects, as well as a JdbcDecoder for mapping rows in the database to Game instances.
