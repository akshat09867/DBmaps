#' Define Metadata for a Data Table in a Tidy data.table
#'
#' Takes descriptive information about a table and returns a tidy data.table.
#'
#' @param table_name Character string, the conceptual name of the table.
#' @param source_identifier Character string, the file name or DB table identifier.
#' @param identifier_columns Character vector, names of column(s) acting as primary key(s).
#' @param key_outcome_specs A list of 'OutcomeSpec' lists.
#' @return A tidy data.table with the table's metadata. The `identifier_columns` and
#'   `grouping_variables` columns are list-columns.
#' @importFrom data.table rbindlist setcolorder data.table
#' @export
#' @examples
#' transactions_info <- table_info(
#'   table_name = "transactions",
#'   source_identifier = "transactions.csv",
#'   identifier_columns = c("customer_id", "product_id", "time"),
#'   key_outcome_specs = list(
#'     list(
#'       OutcomeName = "Revenue",
#'       ValueExpression = quote(price * quantity),
#'       AggregationMethods = list(
#'         list(AggregatedName = "RevenueByCustomer", AggregationFunction = "sum",
#'              GroupingVariables = "customer_id"),
#'         list(AggregatedName = "RevenueByProduct", AggregationFunction = "sum",
#'              GroupingVariables = "product_id")
#'       )
#'     )
#'   )
#' )
#' # Note the structure of the list-columns
#' print(transactions_info)
#' str(transactions_info[, .(identifier_columns, grouping_variable)])
#'
table_info <- function(table_name,
                       source_identifier,
                       identifier_columns,
                       key_outcome_specs) {
  if (!is.character(table_name) || length(table_name) != 1) stop("'table_name' must be a single character string.")
  if (!is.character(source_identifier) || length(source_identifier) != 1) stop("'source_identifier' must be a single character string.")
  if (!is.character(identifier_columns) || length(identifier_columns) == 0) stop("'identifier_columns' must be a character vector with at least one column name.")
  if (!is.list(key_outcome_specs)) stop("'key_outcome_specs' must be a list.")

  rows <- list()
  for (spec in key_outcome_specs) {
    if (!is.character(spec$OutcomeName) || length(spec$OutcomeName) != 1) stop("'OutcomeName' must be a single character string.")
    if (is.null(spec$ValueExpression)) stop("'ValueExpression' must be provided.")
    if (!is.list(spec$AggregationMethods) || length(spec$AggregationMethods) == 0) stop("'AggregationMethods' must be a non-empty list.")

    for (agg in spec$AggregationMethods) {
      if (!is.character(agg$AggregatedName) || length(agg$AggregatedName) != 1) stop("'AggregatedName' must be provided.")
      if (!is.character(agg$AggregationFunction) || length(agg$AggregationFunction) != 1) stop("'AggregationFunction' must be provided.")
      if (!is.character(agg$GroupingVariables) || length(agg$GroupingVariables) == 0) stop("Each 'AggregationMethods' entry must have at least one 'GroupingVariable'.")
      rows[[length(rows) + 1]] <- list(
        table_name = table_name,
        source_identifier = source_identifier,
        identifier_columns = list(identifier_columns),
        outcome_name = spec$OutcomeName,
        value_expression = deparse(spec$ValueExpression),
        aggregated_name = agg$AggregatedName,
        aggregation_function = agg$AggregationFunction,
        grouping_variable = list(agg$GroupingVariables)
      )
    }
  }

  if (length(rows) == 0) {
    return(data.table::data.table(
      table_name = character(),
      source_identifier = character(),
      identifier_columns = list(),
      outcome_name = character(),
      value_expression = character(),
      aggregated_name = character(),
      aggregation_function = character(),
      grouping_variable = list()
    ))
  }

  dt <- data.table::rbindlist(rows)
  data.table::setcolorder(dt, c(
    "table_name", "source_identifier", "identifier_columns",
    "outcome_name", "value_expression", "aggregated_name",
    "aggregation_function", "grouping_variable"
  ))
  dt
}