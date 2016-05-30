import com.khaale.bigdatarampup.testing.fixtures.BiddingFixture
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

val format = DateTimeFormat.forPattern("yyyy.MM.dd")

//val event = BiddingFixture.createEvent()
val dt = new LocalDate(2013,6,9)



dt.toString(format)
dt.toDate.getTime