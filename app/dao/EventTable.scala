package dao

import java.util.Date
import javax.inject._

import models._
import clients.{CassandraClient, CassandraFutureAdapter}
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{ResultSet, Row, Statement}

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
  def save(e: Event): Future[ResultSet] = {
    val statement: Statement = QueryBuilder.
      insertInto("${client.Keyspace}", "$ColumnFamily").
      value("srcid", e.srcId).
      value("ts", e.ts.getTime).
      value("count", e.count)

    client.session.executeAsync(statement)
  }

  /** Get latest Event for a given srcId
    *
    * @param srcId Event srcId
    * @return Some Event, or None if srcId not found
    */
  def getLatest(srcId: String): Future[Option[Event]] = Future {
    val statement =
      """
        |select * from ${client.Keyspace}.$ColumnFamily
        |where srcid = ${srcId}
        |order by ts desc
        |limit 1
      """.stripMargin

    // This loop should execute at most once
    for (row <- client.session.execute(statement)) {
      Some(fromRow(row))
    }

    None
  }

  /**
    * Method to prepare a SQL statement for getting events in a given range.
    *
    * @param from
    * @param to
    * @return
    */
  private def prepareStmt(from: Option[Date], to: Option[Date]): String = {
    val statement = (from, to) match {
      case (None, None) => """
                             |select * from ${client.Keyspace}.$ColumnFamily
                             |where srcid = ${srcId}
                           """.stripMargin
      case (f, None) => """
                          |select * from ${client.Keyspace}.$ColumnFamily
                          |where srcid = ${srcId} and ts >= ${f.getTime}
                        """.stripMargin
      case (None, t) => """
                          |select * from ${client.Keyspace}.$ColumnFamily
                          |where srcid = ${srcId} and ts <= ${t.getTime}
                        """.stripMargin
      case (f, t) => """
                       |select * from ${client.Keyspace}.$ColumnFamily
                       |where srcid = ${srcId} and ts >= ${t.getTime} and ts <= ${t.getTime}
                     """.stripMargin
    }
    statement
  }

  /** Find Events for a srcId within a time range
    *
    * @param srcId Event srcId
    * @param from  A starting timestamp (optional)
    * @param to    An ending timestamp (optional)
    * @return Some sequence of Events, or None if no Events found
    */
  def getRange(srcId: String, from: Option[Date] = None, to: Option[Date] = None): Future[Iterator[Event]] = Future {
    val statement: String = prepareStmt(from, to)

    client.session.execute(statement).map(row => fromRow(row)).toIterator
  }

  /** Summarize a sequence of Events
    *
    * @param srcId Event srcId
    * @param from  A starting timestamp (optional)
    * @param to    An ending timestamp (optional)
    * @return Some summary of Events, or None if not found
    */
  def getSummary(srcId: String, from: Option[Date] = None, to: Option[Date] = None): Future[Option[EventSummary]] = Future {
    val statement: String = prepareStmt(from, to)
    var count: Int = 0
    var sum: Int = 0

    for (result <- client.session.execute(statement)) {
      count += 1
      sum += result.getInt("count")
    }

    if (count > 0) {
      Some(EventSummary(from.getOrElse(null), to.getOrElse(null), count, sum, sum.toDouble / count))
    } else {
      None
    }
  }

  /** Create the Cassandra column family
    *
    * @return ResultSet
    */
  private def createSchema(): ResultSet = client.session.execute(
    s"""
       CREATE TABLE IF NOT EXISTS ${client.Keyspace}.$ColumnFamily (
        srcid text,
        ts timestamp,
        count int,
        PRIMARY KEY(srcid, ts)
        )
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