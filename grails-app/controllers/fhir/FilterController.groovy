package fhir

import javax.sql.DataSource

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
    def group = request.reader.text.decodeFhirJson()
    group.characteristic.each { characteristic ->
      switch(characteristic.code.coding) {
        case {it.any { c -> c.system == "http://loinc.org" && c.code == "21840-4"}}: // Patient Gender
          def firstValueCode = characteristic.valueCodeableConcept.coding[0]
          patientWhere << new WhereClause(searchParam: "gender", column: "token_code", comparator: "=", value: firstValueCode.code)
        break
        case {it.any { c -> c.system == "http://loinc.org" && c.code == "21612-7"}}: // Patient Age
          if (characteristic.valueRange) {
            if (characteristic.valueRange.high) {
              def ageInYears = characteristic.valueRange.high.value.toInteger();
              def d = new Date();
              d[Calendar.YEAR] = (d[Calendar.YEAR] - ageInYears)
              patientWhere << new WhereClause(searchParam: "birthdate", column: "date_max", comparator: ">", value: d.format("YYYY-MM-dd"))
            }
            if (characteristic.valueRange.low) {
              def ageInYears = characteristic.valueRange.low.value.toInteger()
              def d = new Date()
              d[Calendar.YEAR] = d[Calendar.YEAR] - ageInYears
              patientWhere << new WhereClause(searchParam: "birthdate", column: "date_max", comparator: "<", value: d.format("YYYY-MM-dd"))
            }
          }
        break
        case {it.any { c -> c.system == "http://loinc.org" && c.code == "11450-4"}}:
          conditionWhere << new WhereClause(searchParam: "code", column: "token_code", comparator: "=", value: characteristic.valueCodeableConcept.coding[0].code,
                                            system: characteristic.valueCodeableConcept.coding[0].system)
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
