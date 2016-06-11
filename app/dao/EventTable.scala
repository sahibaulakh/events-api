package dao

import java.util.Date
import javax.inject._

import models._
import clients.{CassandraFutureAdapter, CassandraClient}
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{ResultSet, Row}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

@Singleton
class EventTable @Inject()(client: CassandraClient) extends CassandraFutureAdapter {
  val ColumnFamily = "events"

  this.createSchema()

  /** Save an Event model to Cassandra table
    *
    * @param e Event class
    * @return A Cassandra ResultSet
    */
  def save(e: Event): Future[ResultSet] = ???

  /** Get latest Event for a given srcId
    *
    * @param srcId Event srcId
    * @return Some Event, or None if srcId not found
    */
  def getLatest(srcId: String): Future[Option[Event]] = ???

  /** Find Events for a srcId within a time range
    *
    * @param srcId Event srcId
    * @param from A starting timestamp (optional)
    * @param to An ending timestamp (optional)
    * @return Some sequence of Events, or None if no Events found
    */
  def getRange(srcId: String, from: Option[Date] = None, to: Option[Date] = None): Future[Iterator[Event]] = ???

  /** Summarize a sequence of Events
    *
    * @param srcId Event srcId
    * @param from A starting timestamp (optional)
    * @param to An ending timestamp (opttional)
    * @return Some summary of Events, or None if not found
    */
  def getSummary(srcId: String, from: Option[Date] = None, to: Option[Date] = None): Future[Option[EventSummary]] = ???

  /** Create the Cassandra column family
    *
    * @return ResultSet
    */
  private def createSchema(): ResultSet = client.session.execute(
    s"""
       |CREATE TABLE IF NOT EXISTS ${client.Keyspace}.$ColumnFamily (
       |  ???
      """.stripMargin
  )

  /** Convert a Cassandra Row into an Event model
    *
    * @param row Cassandra Row (from query result)
    * @return Event model
    */
  private def fromRow(row: Row): Event = {
    Event(
      srcId = row.getString("srcid"),
      ts = row.getDate("ts"),
      count = row.getInt("count")
    )
  }

}