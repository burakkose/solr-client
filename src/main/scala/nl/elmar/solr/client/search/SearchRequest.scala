package nl.elmar.solr.client.search

import akka.http.scaladsl.model._
import play.api.libs.json._
import play.api.libs.functional.syntax._

case class SearchRequest(
    collection: String,
    body: SearchRequestBody = SearchRequestBody()
)

object SearchRequest {
  def toHttpRequest(solrUri: Uri, request: SearchRequest): HttpRequest = {
    HttpRequest(
      uri = solrUri withPath solrUri.path / request.collection / "query",
      entity =
        HttpEntity(
          ContentTypes.`application/json`,
          bodyWriter.writes(request.body).toString
        )
    )
  }

  import nl.elmar.solr.client.CommonWriters._

  def renderOrUnrolled(exp: FilterExpression): String = {
    exp match {
      case FilterExpression.OR(left: FilterExpression, right: FilterExpression) =>
        s"${renderOrUnrolled(left)} OR ${renderOrUnrolled(right)}"
      case other: FilterExpression =>
        renderFilterExpression(other)
    }
  }

  def renderAndUnrolled(exp: FilterExpression): String = {
    exp match {
      case FilterExpression.AND(left: FilterExpression, right: FilterExpression) =>
        s"${renderAndUnrolled(left)} AND ${renderAndUnrolled(right)}"
      case other: FilterExpression =>
        renderFilterExpression(other)
    }
  }

  def renderFilterExpression(value: FilterExpression): String = value match {
    case FilterExpression.Term.String(v) => v.replace(":", """\:""")
    case FilterExpression.Term.Long(v) => v.toString
    case FilterExpression.Term.Date(v) => renderDate(v)
    case FilterExpression.Range(fromOpt, toOpt) =>
      val from = fromOpt.map(renderFilterExpression).getOrElse("*")
      val to = toOpt.map(renderFilterExpression).getOrElse("*")
      s"[$from TO $to]"
    case FilterExpression.OR(left, right) =>
      s"(${renderOrUnrolled(left)} OR ${renderOrUnrolled(right)})"
    case FilterExpression.AND(left, right) =>
      s"(${renderAndUnrolled(left)} AND ${renderAndUnrolled(right)})"
    case FilterExpression.NOT(expression) =>
      s"(NOT ${renderFilterExpression(expression)})"
  }

  def renderFilterDefinition(filterDefinition: FilterDefinition) = {
    val FilterDefinition(fieldName, exp, tagOpt) = filterDefinition
    val tag = tagOpt map (tag => s"{!tag=$tag}") getOrElse ""
    s"$tag$fieldName:${renderFilterExpression(exp)}"
  }

  implicit val sortingWriter = Writes[Sorting] {
    case Sorting(field, order) =>
      val ord = order match {
        case SortOrder.Asc => "asc"
        case SortOrder.Desc => "desc"
      }
      JsString(s"$field $ord")
  }

  implicit val filterDefinitionWriter = Writes[FilterDefinition] { fd =>
    JsString(renderFilterDefinition(fd))
  }

  implicit val facetMetadataDomainWriter: Writes[FacetMetadata.Domain] =
    (__ \ "excludeTags").writeNonEmptyList[String].contramap(_.excludeTags)

  implicit val termsMetadataWriter: Writes[FacetMetadata.Terms] = (
    (__ \ "type").write[String] and
      (__ \ "field").write[String] and
      (__ \ "limit").writeNullable[Long] and
      (__ \ "sort").writeNullable[Sorting] and
      (__ \ "facet").lazyWriteNonEmptyList(facetListWriter) and
      (__ \ "domain").writeNullable[FacetMetadata.Domain]
    )(m => ("terms", m.field, m.limit, m.sort, m.subFacets, m.domain))

  implicit val rangeIncludeWriter: Writes[FacetMetadata.Range.Include] = Writes( include =>
    JsString(include.toString.toLowerCase)
  )

  implicit val rangeMetadataWriter: Writes[FacetMetadata.Range] = (
    (__ \ "type").write[String] and
      (__ \ "field").write[String] and
      (__ \ "start").write[Long] and
      (__ \ "end").write[Long] and
      (__ \ "gap").write[Long] and
      (__ \ "sort").writeNullable[Sorting] and
      (__ \ "include").writeNonEmptyList[FacetMetadata.Range.Include] and
      (__ \ "facet").lazyWriteNonEmptyList(facetListWriter) and
      (__ \ "domain").writeNullable[FacetMetadata.Domain]
    )(m => ("range", m.field, m.start, m.end, m.gap, m.sort, m.include, m.subFacets, m.domain))

  implicit val uniqueMetadataWriter = Writes[FacetMetadata.Unique]{
    case FacetMetadata.Unique(field, function) =>
      JsString(s"$function($field)")
  }

  implicit val facetMetadataWriter = Writes[FacetMetadata] {
    case terms: FacetMetadata.Terms => Json toJson terms
    case range: FacetMetadata.Range => Json toJson range
    case unique: FacetMetadata.Unique => Json toJson unique
  }

  val facetListWriter = OWrites[List[FacetDefinition]] {
    case Nil => JsObject.empty
    case nonEmpty =>
      nonEmpty.foldLeft(JsObject.empty) {
        case (obj, FacetDefinition(name, metadata)) =>
          obj + (name -> facetMetadataWriter.writes(metadata))
      }
  }

  implicit val resultGroupingWriter: Writes[ResultGrouping] = (
    (__ \ "group").write[Boolean] and
      (__ \ "group.field").write[String] and
      (__ \ "group.sort").writeNullable[Sorting]
    )(rg => (true, rg.field, rg.sort))

  val routingListWriter: Writes[List[String]] = { list =>
    JsString(list.map(_ + "!").mkString(","))
  }

  implicit val bodyWriter: Writes[SearchRequestBody] = (
    (__ \ "query").write[String] and
      (__ \ "filter").writeNonEmptyList[FilterDefinition] and
      (__ \ "params" \ "_route_").writeNonEmptyList[String](routingListWriter) and
      (__ \ "params" \ "sort").writeNullable[Sorting] and
      (__ \ "params" \ "start").writeNullable[Long] and
      (__ \ "params" \ "rows").writeNullable[Long] and
      (__ \ "params").writeNullable[ResultGrouping] and
      (__ \ "facet").writeNonEmptyList(facetListWriter)
    )(q => ("*:*", q.filter, q.routing, q.sort, q.start, q.rows, q.grouping, q.facets))
}

