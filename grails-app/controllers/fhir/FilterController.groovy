package fhir

import javax.sql.DataSource
import groovy.json.JsonSlurper

class WhereClause {
  String searchParam
  String column
  String comparator
  String value
  String system
}

class FilterController {

  static scope = "singleton"
  SearchIndexService searchIndexService
  SqlService sqlService
  UrlService urlService

  DataSource dataSource

  def instaCount() {
    def patientWhere = []
    def conditionWhere = []
    def slurper = new JsonSlurper()
    def filter = slurper.parseText(request.reader.text)
    filter.parameter.each { param ->
      switch(param.url) {
        case "http://interventionengine.org/patientgender":
          patientWhere << new WhereClause(searchParam: "gender", column: "token_code", comparator: "=", value: param.valueString)
        break
        case "http://interventionengine.org/patientage":
          if (param.valueRange) {
            if (param.valueRange.high) {
              def ageInYears = param.valueRange.high.value.toInteger();
              def d = new Date();
              d[Calendar.YEAR] = (d[Calendar.YEAR] - ageInYears)
              patientWhere << new WhereClause(searchParam: "birthdate", column: "date_max", comparator: ">", value: d.format("YYYY-MM-dd"))
            }
            if (param.valueRange.low) {
              def ageInYears = param.valueRange.low.value.toInteger()
              def d = new Date()
              d[Calendar.YEAR] = d[Calendar.YEAR] - ageInYears
              patientWhere << new WhereClause(searchParam: "birthdate", column: "date_max", comparator: "<", value: d.format("YYYY-MM-dd"))
            }
          }
        break
        case "http://interventionengine.org/conditioncode":
          conditionWhere << new WhereClause(searchParam: "code", column: "token_code", comparator: "=", value: param.valueCodeableConcept.coding[0].code,
                                            system: param.valueCodeableConcept.coding[0].system)
        break
      }
    }
    def query = new StringBuilder("select fhir_id from (")
    query.append(patientWhere.collect { pw ->
      "select fhir_id from resource_index_term where fhir_type = 'Patient' AND search_param = '$pw.searchParam' AND $pw.column $pw.comparator '$pw.value'"
    }.join("\nINTERSECT\n"))
    query.append(") p")
    if (conditionWhere.size() > 0) {
      query.append(" WHERE\n")
    }
    query.append(conditionWhere.collect { qw ->
      """EXISTS(
          SELECT fhir_id
            from resource_index_term
            where fhir_type = 'Condition'  AND search_param = '$qw.searchParam'  AND token_namespace = '$qw.system' AND $qw.column = '$qw.value'
          INTERSECT

          SELECT fhir_id
            from resource_index_term
            where fhir_type = 'Condition'  AND search_param = 'patient'  AND reference_id = p.fhir_id)
    """
    }.join("\nAND\n"))

    def response = sqlService.query(query.toString()).collect {i -> i.fhir_id}
    render response
  }

}
