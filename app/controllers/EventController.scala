package controllers

import java.util.Date
import javax.inject._

import dao._
import models._
import play.api.libs.json.Json
import play.api.mvc._

import scala.concurrent.ExecutionContext.Implicits.global

@Singleton
class EventController @Inject()(table: EventTable) extends Controller {

  /** Save Event JSON to Cassandra table
    *
    * @return 201 Created
    */
  def save() = Action.async(parse.json[Event]){ request =>
    val event = request.body
    table.save(event).map(_ => Ok("Saved event"))
  }

  /** Return most recent Event JSON for a given srcId
    *
    * @param srcId Event srcId
    * @return 200 OK (Event JSON) or 404 Not Found
    */
  def getLatest(srcId: String) = Action.async{
    table.getLatest(srcId).map(event => Ok(Json.format[Event]))
  }

  /** Return an array of Event JSON for a given srcId within a time range
    *
    * @param srcId Event srcId
    * @param from UTC Timestamp (Long) start -- optional
    * @param to UTC Timestamp (Long) end -- optional
    * @return 200 OK (Event JSON array) or 404 Not Found
    */
  def getRange(srcId: String, from: Option[Long], to: Option[Long]) = Action.async{
    val fromDate = from.map(v => new Date(v))
    val toDate = to.map(v => new Date(v))
    table.getRange(srcId, fromDate, toDate).map(event => Ok(Json.format[Event]))
  }

  /** Return an EventSummary JSON for a given srcId within a time range
    *
    * @param id Event srcId
    * @param from UTC Timestamp (Long) start -- optional
    * @param to UTC Timestamp (Long) end -- optional
    * @return 200 OK (EventSummary JSON) or 404 Not Found
    */
  def getSummary(id: String, from: Option[Long], to: Option[Long]) = Action.async{
    val fromDate = from.map(v => new Date(v))
    val toDate = to.map(v => new Date(v))
    table.getSummary(id, fromDate, toDate).map(summary => Ok(Json.format[EventSummary]))
  }
}