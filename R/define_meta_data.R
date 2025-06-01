#' Define Metadata for a Data Table
#'
#' Creates a structured list containing metadata for a single data table,
#' including how its key outcomes can be aggregated.
#'
#' @param table_name Character string, the conceptual name of the table.
#' @param source_identifier Character string, the file name or DB table identifier.
#' @param description Character string, a brief description of the table (optional).
#' @param identifier_columns Character vector, names of column(s) acting as primary key(s).
#' @param key_outcome_specs A list of 'OutcomeSpec' lists. Each OutcomeSpec should be a list
#'   with elements:
#'   \itemize{
#'     \item `OutcomeName` (character): Conceptual name of the outcome.
#'     \item `ValueExpression` (language or character): R expression (use `quote()`) or string
#'       for the per-row value calculation.
#'     \item `AggregationMethods` (list of 'AggregationSpec' lists):
#'       Each AggregationSpec should be a list with elements:
#'       \itemize{
#'         \item `AggregatedName` (character): Descriptive name for the aggregated result.
#'         \item `AggregationFunction` (character): R aggregation function (e.g., "sum").
#'         \item `GroupingVariables` (character vector): Columns to group by. Use
#'           `character(0)` or `NULL` for no grouping.
#'       }
#'   }
#' @return A list object representing the metadata for the table.
#' @export
#' @examples
#' transactions_metadata <- define_table_meta(
#'   table_name = "transactions",
#'   source_identifier = "transactions.csv",
#'   description = "Table of individual sales transactions.",
#'   identifier_columns = "transaction_id",
#'   key_outcome_specs = list(
#'     list( # Outcome: Revenue
#'       OutcomeName = "Revenue",
#'       ValueExpression = quote(price * quantity),
#'       AggregationMethods = list(
#'         list(AggregatedName = "TotalRevenueByCustomer",
#'              AggregationFunction = "sum",
#'              GroupingVariables = "customer_id"),
#'         list(AggregatedName = "TotalRevenueByProduct",
#'              AggregationFunction = "sum",
#'              GroupingVariables = "product_id"),
#'         list(AggregatedName = "OverallTotalRevenue",
#'              AggregationFunction = "sum",
#'              GroupingVariables = character(0))
#'       )
#'     ),
#'     list( # Outcome: Transaction Count
#'       OutcomeName = "TransactionCount",
#'       ValueExpression = 1, # Each row is one transaction
#'       AggregationMethods = list(
#'         list(AggregatedName = "CountTransactionsByCustomer",
#'              AggregationFunction = "sum", # sum of 1s is a count
#'              GroupingVariables = "customer_id"),
#'         list(AggregatedName = "CountTransactionsByProduct",
#'              AggregationFunction = "sum",
#'              GroupingVariables = "product_id")
#'       )
#'     )
#'   )
#' )
#' print(transactions_metadata)
define_table_meta <- function(table_name,
                              source_identifier,
                              description = NULL,
                              identifier_columns,
                              key_outcome_specs) {

  if (!is.character(table_name) || length(table_name) != 1) {
    stop("'table_name' must be a single character string.")
  }
  if (!is.character(source_identifier) || length(source_identifier) != 1) {
    stop("'source_identifier' must be a single character string.")
  }
  if (!is.null(description) && (!is.character(description) || length(description) != 1)) {
    stop("'description' must be NULL or a single character string.")
  }
  if (!is.character(identifier_columns) || length(identifier_columns) == 0) {
    stop("'identifier_columns' must be a character vector with at least one column name.")
  }
  if (!is.list(key_outcome_specs)) {
    stop("'key_outcome_specs' must be a list.")
  }


  entry <- list(
    TableName = table_name,
    SourceIdentifier = source_identifier,
    Description = description,
    IdentifierColumns = identifier_columns,
    KeyOutcomeSpecs = key_outcome_specs
  )


  return(entry)
}