package org.example

import org.postgresql.util.PSQLException
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Statement
import java.time.LocalDate
import java.util.concurrent.ThreadLocalRandom
import kotlin.random.Random

class Generator(url: String, login: String, password: String) {
    private lateinit var connection: Connection
    private lateinit var statement: Statement
    init {
        try {
            connection = DriverManager.getConnection(url, login, password)
            statement = connection.createStatement()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    companion object {
        private val LETTERS = ('a'..'z').toList()

        fun randomDate(notBeforeDate: LocalDate = LocalDate.of(2000, 1, 1),
                       notAfterDate: LocalDate = LocalDate.of(2100, 1, 1)): LocalDate {
            val start = notBeforeDate.toEpochDay()
            val end = notAfterDate.toEpochDay()
            val randomEpochDay = ThreadLocalRandom.current().longs(start, end).findAny().asLong
            return LocalDate.ofEpochDay(randomEpochDay)
        }

        fun randomPhone(): String {
            return ("+ " + Random.nextInt(1, 999).toString() + " " + randomNumberSequence(3) + " " +
                    randomNumberSequence(7))
        }

        fun randomNumberSequence(length: Int = Random.nextInt(1,7)): String =
            generateSequence(Random.nextInt(0, 10).toString()) { it + Random.nextInt(0, 10).toString() }.elementAt(length - 1)

        fun randomWord(length: Int = Random.nextInt(1,14)): String =
            generateSequence(LETTERS.random().toString()) { it + LETTERS.random().toString() }.elementAt(length - 1)

        fun randomText(length: Int = Random.nextInt(1, 15)): String =
            generateSequence(randomWord()) { it + " " + randomWord() }.elementAt(length - 1)

        fun generateInsert(table: String, columns: ArrayList<String>, data: ArrayList<String>): String {
            val columnsString = columns.joinToString(separator=", ", prefix="(", postfix=")")
            val dataString = data.joinToString(separator="', '", prefix="('", postfix="')")
            return "INSERT INTO \"$table\" $columnsString VALUES $dataString"
        }
    }

    fun disconnect() {
        statement.close()
        connection.close()
    }

    fun randomForeignKey(table: String, column: String): String {
        val records: MutableList<String> = ArrayList()
        try {
            val recordsResultSet = statement.executeQuery("SELECT $column FROM $table")
            while (recordsResultSet.next()) {
                val value = recordsResultSet.getString(column)
                records.add(value)
            }
        } catch (e: SQLException) {
            e.printStackTrace()
        }
        check (records.isNotEmpty()) {"list id is empty"}
        return records.random()
    }

    fun addRegisteredPersons(amount: Int) {
        for (i in 1..amount) {
            val columns = arrayListOf<String>()
            val data = arrayListOf<String>()
            columns.add(RegisteredPersons.Columns.SURNAME)
            data.add(randomWord().capitalize())
            columns.add(RegisteredPersons.Columns.NAME)
            data.add(randomWord().capitalize())
            if (Random.nextDouble() < 0.95) {
                columns.add(RegisteredPersons.Columns.PARENTAL_NAME)
                data.add(randomWord().capitalize())
            }
            if (Random.nextDouble() < 0.95) {
                columns.add(RegisteredPersons.Columns.PHONE_NUMBER)
                data.add(randomPhone().capitalize())
            }
            statement.executeUpdate(generateInsert(RegisteredPersons.TABLE_NAME, columns, data))
        }
    }

    fun addOrganisations(amount: Int) {
        for (i in 1..amount) {
            val columns = arrayListOf<String>()
            val data = arrayListOf<String>()
            columns.add(Organisations.Columns.NAME)
            data.add(randomWord().capitalize())
            if (Random.nextDouble() < 0.95) {
                columns.add(Organisations.Columns.BALANCE)
                data.add(Random.nextInt(1,100_000_000).toString())
            }
            if (Random.nextDouble() < 0.95) {
                columns.add(Organisations.Columns.CHARACTERISTICS)
                data.add(randomText().capitalize())
            }
            statement.executeUpdate(generateInsert(Organisations.TABLE_NAME, columns, data))
        }
    }

    fun addExecutives(amount: Int) {
        var i = 1
        while (i <= amount) {
            val columns = arrayListOf<String>()
            val data = arrayListOf<String>()
            try {
                columns.add(Executives.Columns.ID)
                data.add(randomForeignKey(RegisteredPersons.TABLE_NAME, RegisteredPersons.Columns.ID))
                if (Random.nextDouble() < 0.95) {
                    columns.add(Executives.Columns.CHARACTERISTICS)
                    data.add(randomText().capitalize())
                }
                statement.executeUpdate(generateInsert(Executives.TABLE_NAME, columns, data))
                i++
            } catch(e: PSQLException) {
                if (e.sqlState != UNIQUE_VIOLATION)  //	unique_violation - если случайно сгенерировали уже существующий ключ
                    throw PSQLException(e.serverErrorMessage)  // то генерируем повторно, иначе - исключение
            }
        }
    }

    fun addEventTypes(amount: Int) {
        var i = 1
        while (i <= amount) {
            try {
                val columns = arrayListOf<String>()
                val data = arrayListOf<String>()
                columns.add(Event_type.Columns.TYPE)
                data.add(randomWord())
                columns.add(Event_type.Columns.DESCRIPTION)
                data.add(randomText())
                statement.executeUpdate(generateInsert(Event_type.TABLE_NAME, columns, data))
                i++
            } catch(e: PSQLException) {
                if (e.sqlState != UNIQUE_VIOLATION)  //	unique_violation - если случайно сгенерировали уже существующий ключ
                    throw PSQLException(e.serverErrorMessage)  // то генерируем повторно, иначе - исключение
            }
        }
    }

    fun addPlaces(amount: Int) {
        for (i in 1..amount) {
            val columns = arrayListOf<String>()
            val data = arrayListOf<String>()
            if (Random.nextDouble() < 0.95) {
                columns.add(Places.Columns.ADDRESS)
                data.add(randomText().capitalize())
            }
            columns.add(Places.Columns.DESCRIPTION)
            data.add(randomText().capitalize())
            if (Random.nextDouble() < 0.95) {
                columns.add(Places.Columns.OWNER_ID)
                data.add(randomForeignKey(Organisations.TABLE_NAME, Organisations.Columns.ID))
            }
            statement.executeUpdate(generateInsert(Places.TABLE_NAME, columns, data))
        }
    }

    fun addEvents(amount: Int) {
        for (i in 1..amount) {
            val columns = arrayListOf<String>()
            val data = arrayListOf<String>()
            columns.add(Events.Columns.NAME)
            data.add(randomText(Random.nextInt(1,6)).capitalize())
            columns.add(Events.Columns.TYPE)
            data.add(randomForeignKey(Event_type.TABLE_NAME, Event_type.Columns.TYPE))
            val starting_date = randomDate()
            columns.add(Events.Columns.STARTING_DATE)
            data.add(starting_date.toString())
            columns.add(Events.Columns.FINISHING_DATE)
            data.add(randomDate(starting_date, starting_date.plusMonths(1)).toString())
            if (Random.nextDouble() < 0.95) {
                columns.add(Events.Columns.RESPONSIBLE_ID)
                data.add(randomForeignKey(Executives.TABLE_NAME, Executives.Columns.ID))
            }
            if (Random.nextDouble() < 0.95) {
                columns.add(Events.Columns.ORGANIZER_ID)
                data.add(randomForeignKey(Organisations.TABLE_NAME, Organisations.Columns.ID))
            }
            var maxBudget = 100000.toBigInteger()
            if (Random.nextDouble() < 0.95) {
                columns.add(Events.Columns.SPONSOR_ID)
                val sponsor_id = randomForeignKey(Organisations.TABLE_NAME, Organisations.Columns.ID).toInt()
                data.add(sponsor_id.toString())
                val recordsResultSet = statement.executeQuery("SELECT " + Organisations.Columns.ID +
                        ", " + Organisations.Columns.BALANCE + " FROM " + Organisations.TABLE_NAME)
                while (recordsResultSet.next()) {
                    val newId = recordsResultSet.getString(Organisations.Columns.ID).toInt()
                    if (newId == sponsor_id)
                        maxBudget =
                            (recordsResultSet.getString(Organisations.Columns.BALANCE) ?: "100000").toBigInteger()
                }
            }
            if (Random.nextDouble() < 0.95) {
                columns.add(Events.Columns.PLACE_ID)
                data.add(randomForeignKey(Places.TABLE_NAME, Places.Columns.ID))
            }
            columns.add(Events.Columns.BUDGET)
            data.add(Random.nextInt(0, maxBudget.toInt()).toString())

            statement.executeUpdate(generateInsert(Events.TABLE_NAME, columns, data))
        }
    }

    fun addVisits(amount: Int) {
        for (i in 1..amount) {
            val columns = arrayListOf<String>()
            val data = arrayListOf<String>()
            columns.add(Visits.Columns.PERSON_ID)
            data.add(randomForeignKey(RegisteredPersons.TABLE_NAME, RegisteredPersons.Columns.ID))
            columns.add(Visits.Columns.EVENT_ID)
            data.add(randomForeignKey(Events.TABLE_NAME, Events.Columns.ID))
            statement.executeUpdate(generateInsert(Visits.TABLE_NAME, columns, data))
        }
    }

    fun addReviews(amount: Int) {
        for (i in 1..amount) {
            val columns = arrayListOf<String>()
            val data = arrayListOf<String>()
            columns.add(Reviews.Columns.VISIT_ID)
            data.add(randomForeignKey(Visits.TABLE_NAME, Visits.Columns.ID))
            columns.add(Reviews.Columns.TEXT)
            data.add(randomText().capitalize())
            statement.executeUpdate(generateInsert(Reviews.TABLE_NAME, columns, data))
        }
    }
}
