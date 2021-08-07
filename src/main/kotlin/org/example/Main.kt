package org.example

import org.postgresql.util.PSQLException
import java.math.BigInteger
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.time.LocalDate
import java.util.concurrent.ThreadLocalRandom
import kotlin.random.Random


fun main(args: Array<String>) {
    val gen = Generator("jdbc:postgresql://localhost:5432/culture_fest", login, password)
    //gen.addRegisteredPersons(10000)
    //gen.addOrganisations(100)
    //gen.addExecutives(100)
    //gen.addEventTypes(5)
    //gen.addPlaces(20)
    gen.addEvents(1000)
    gen.addVisits(20000)
    gen.addReviews(4000)
    gen.disconnect()
}