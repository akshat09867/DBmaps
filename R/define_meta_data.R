#' Define Metadata for a Data Table in a Tidy data.table
#'
#' Takes descriptive information about a table and its analytical outcomes and
#' returns a tidy data.table where each row represents a single, unique
#' aggregation method for a specific key outcome. This function uses the
#' data.table package for performance.
#'
#' @param table_name Character string, the conceptual name of the table.
#' @param source_identifier Character string, the file name or DB table identifier.
#' @param identifier_columns Character vector, names of column(s) acting as primary key(s).
#' @param key_outcome_specs A list of 'OutcomeSpec' lists. Each OutcomeSpec is a list
#'   with elements `OutcomeName`, `ValueExpression` (use `quote()`), and
#'   `AggregationMethods` (a list of 'AggregationSpec' lists).
#' @return A tidy data.table with the table's metadata, flattened so each row
#'   is a unique aggregation specification.
#' @importFrom data.table rbindlist as.data.table
#' @export
#' @examples
#' transactions_info <- table_info(
#'   table_name = "transactions",
#'   source_identifier = "transactions.csv",
#'   identifier_columns = c("customer_id", "product_id", "time"),
#'   key_outcome_specs = list(
#'     list( # Outcome: Revenue
#'       OutcomeName = "Revenue",
#'       ValueExpression = quote(price * quantity),
#'       AggregationMethods = list(
#'         list(AggregatedName = "TotalRevenueByCustomer", AggregationFunction = "sum",
#'              GroupingVariables = "customer_id"),
#'         list(AggregatedName = "TotalRevenueByProduct", AggregationFunction = "sum",
#'              GroupingVariables = "product_id")
#'       )
#'     ),
#'     list( # Outcome: Units Sold
#'       OutcomeName = "UnitsSold",
#'       ValueExpression = quote(quantity),
#'       AggregationMethods = list(
#'         list(AggregatedName = "TotalUnitsSoldByProduct", AggregationFunction = "sum",
#'              GroupingVariables = "product_id")
#'       )
#'     )
#'   )
#' )
#' print(transactions_info)
#' class(transactions_info)
table_info <- function(table_name,
                       source_identifier,
                       identifier_columns,
                       key_outcome_specs) {

  if (!is.character(table_name) || length(table_name) != 1) {
    stop("'table_name' must be a single character string.")
  }
  if (!is.character(source_identifier) || length(source_identifier) != 1) {
    stop("'source_identifier' must be a single character string.")
  }
  if (!is.character(identifier_columns) || length(identifier_columns) == 0) {
    stop("'identifier_columns' must be a character vector with at least one column name.")
  }
  if (!is.list(key_outcome_specs)) {
    stop("'key_outcome_specs' must be a list.")
  }
  all_rows <- lapply(key_outcome_specs, function(out_spec){
    basic_info <- list(
      outcome_name = out_spec$OutcomeName,
      value_expression = deparse(out_spec$ValueExpression)
    )

    agg_r <- lapply(out_spec$AggregationMethods, function(agg_spec){
      c(
        basic_info,
        list(
          aggregated_name = agg_spec$AggregatedName,
          aggregation_function = agg_spec$AggregationFunction,
          grouping_variables= if(length(agg_spec$GroupingVariables)>0){
            paste(agg_spec$GroupingVariables, collapse = ",")
         } 
         else {
          NA_character_
         }
        )
      )
    })

return(agg_r)
  })

  flat_list <- unlist(all_rows, recursive = FALSE)
  if(length(flat_list)==0){
    return(
      data.table(
        table_name=character(),
        source_identifier=character(),
        identifier_columns=character(),
        outcome_name = character(),
        value_expression = character(),
        aggregated_name = character(),
        aggregation_function = character(),
        grouping_variables = character()
      )
    )
  }
  final_dt <- data.table::rbindlist(flat_list)
   final_dt[, `:=`(
    table_name = table_name,
    source_identifier = source_identifier,
    identifier_columns = paste(identifier_columns, collapse = ",")
  )]
  data.table::setcolorder(final_dt, c("table_name", "source_identifier", "identifier_columns", "outcome_name", "value_expression", "aggregated_name", "aggregation_function", "grouping_variables"))
  return(final_dt)
}
