package clients

import javax.inject.Singleton

import com.datastax.driver.core._
import com.google.common.util.concurrent.{Futures, FutureCallback}
import org.cassandraunit.utils.EmbeddedCassandraServerHelper

import scala.concurrent.{Future, Promise}

/**
  * A Cassandra "client" object that automatically starts an embedded node
  */
@Singleton
class CassandraClient {
  EmbeddedCassandraServerHelper.startEmbeddedCassandra()

  val Keyspace = "events_api_v1"

  private lazy val cluster = Cluster.builder().addContactPoint("localhost").withPort(9142).build()

  lazy val session: Session = {
    val createKeyspace =
      s"""
        |CREATE KEYSPACE IF NOT EXISTS $Keyspace
        |WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
      """.stripMargin

    val initSession = cluster.connect()
    initSession.execute(createKeyspace)
    initSession.close()

    cluster.connect(Keyspace)
  }

  def close() = {
    session.close()
    cluster.close()
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
  }
}

trait CassandraFutureAdapter {

  /** Adapts Cassandra future result to Scala native Future
    *
    * @param f FutureResultSet from execute.async call
    * @return Scala future of ResultSet
    */
  implicit def resultSetFutureToScala(f: ResultSetFuture): Future[ResultSet] = {
    val p = Promise[ResultSet]()
    Futures.addCallback(f,
      new FutureCallback[ResultSet] {
        def onSuccess(r: ResultSet) = p success r
        def onFailure(t: Throwable) = p failure t
      })
    p.future
  }
}