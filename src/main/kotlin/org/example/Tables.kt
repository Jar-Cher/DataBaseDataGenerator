package org.example

const val UNIQUE_VIOLATION = "23505"

object RegisteredPersons {
    const val TABLE_NAME = "registered_persons"
    object Columns {
        const val ID = "id"
        const val NAME = "name"
        const val SURNAME = "surname"
        const val PARENTAL_NAME = "parental_name"
        const val PHONE_NUMBER = "phone_number"
    }
}
object Organisations {
    const val TABLE_NAME = "organisations"
    object Columns {
        const val ID = "id"
        const val NAME = "name"
        const val BALANCE = "balance"
        const val CHARACTERISTICS = "characteristics"
    }
}
object Executives {
    const val TABLE_NAME = "executives"
    object Columns {
        const val ID = "id"
        const val CHARACTERISTICS = "characteristics"
    }
}
object Event_type {
    const val TABLE_NAME = "event_type"
    object Columns {
        const val TYPE = "type"
        const val DESCRIPTION = "description"
    }
}
object Places {
    const val TABLE_NAME = "places"
    object Columns {
        const val ID = "id"
        const val ADDRESS = "address"
        const val DESCRIPTION = "description"
        const val OWNER_ID = "owner_id"
    }
}
object Events {
    const val TABLE_NAME = "events"
    object Columns {
        const val ID = "id"
        const val NAME = "name"
        const val TYPE = "type"
        const val STARTING_DATE = "starting_date"
        const val FINISHING_DATE = "finishing_date"
        const val RESPONSIBLE_ID = "responsible_id"
        const val ORGANIZER_ID = "organizer_id"
        const val SPONSOR_ID = "sponsor_id"
        const val PLACE_ID = "place_id"
        const val BUDGET = "budget"
    }
}
object Visits {
    const val TABLE_NAME = "visits"
    object Columns {
        const val ID = "id"
        const val PERSON_ID = "person_id"
        const val EVENT_ID = "event_id"
    }
}
object Reviews {
    const val TABLE_NAME = "reviews"
    object Columns {
        const val ID = "id"
        const val VISIT_ID = "visit_id"
        const val TEXT = "text"
    }
}