#' Generate data.table Aggregation Code from Metadata
#'
#' Reads metadata from a master data.table and generates executable
#' data.table code strings for performing aggregations.
#'
#' @param table_name_filter Character string, the name of the table for which to
#'   generate aggregation code.
#' @param metadata_dt A data.table containing the master metadata, created by
#'   calling `table_info()` for multiple tables and combining them.
#' @return A named character vector where each element is a runnable
#'   `data.table` code string, and the names correspond to the grouping variables.
#' @importFrom data.table as.data.table
#' @export
#' @examples
#' # First, create some metadata
#' customers_info <- table_info(
#'   table_name = "customers",
#'   source_identifier = "customers.csv",
#'   identifier_columns = "customer_id",
#'   key_outcome_specs = list(
#'     list(OutcomeName = "CustomerCount", ValueExpression = 1, AggregationMethods = list(
#'       list(AggregatedName = "CountByRegion", AggregationFunction = "sum",
#'            GroupingVariables = "region")
#'     ))
#' ))
#'
#' transactions_info <- table_info(
#'   table_name = "transactions",
#'   source_identifier = "transactions.csv",
#'   identifier_columns = "transaction_id",
#'   key_outcome_specs = list(
#'     list(OutcomeName = "Revenue", ValueExpression = quote(amount), AggregationMethods = list(
#'       list(AggregatedName = "RevenueByCustomer", AggregationFunction = "sum",
#'            GroupingVariables = "customer_id"),
#'       list(AggregatedName = "RevenueByProduct", AggregationFunction = "sum",
#'            GroupingVariables = "product_id")
#'     )),
#'     list(OutcomeName = "Transactions", ValueExpression = 1, AggregationMethods = list(
#'       list(AggregatedName = "TransactionsByCustomer", AggregationFunction = "sum",
#'            GroupingVariables = "customer_id")
#'     ))
#' ))
#'
#' master_metadata <- data.table::rbindlist(list(customers_info, transactions_info))
#'
#' # Now, generate the code for the "transactions" table
#' generated_code <- generate_aggregation_code("transactions", master_metadata)
#' print(generated_code)
#'
#' # To demonstrate execution:
#' # 1. Create the sample data
#' transactions <- data.table::data.table(
#'   transaction_id = c("T001", "T002", "T003"),
#'   customer_id = c("C001", "C002", "C001"),
#'   product_id = c("P001", "P002", "P001"),
#'   amount = c(10.0, 20.0, 15.0)
#' )
#'
#' # 2. Parse and evaluate the first generated statement
#' revenue_by_customer_code <- generated_code["customer_id"]
#' cat("Executing code:\n", revenue_by_customer_code)
#' revenue_by_customer_dt <- eval(parse(text = revenue_by_customer_code))
#' print(revenue_by_customer_dt)
#'

generate_aggregation_code <- function(table_name_filter, metadata_dt) {
  # Ensure data.table context
  dt <- metadata_dt

  # Filter the metadata for the specific table
  table_meta <- dt[dt$table_name == table_name_filter]
  if (nrow(table_meta) == 0) {
    warning("No metadata found for table: ", table_name_filter)
    return(character(0))
  }
  
  # Create grouping keys
  group_keys <- sapply(table_meta$grouping_variable, function(x) {
    sorted_vars <- sort(unlist(x))
    paste(sorted_vars, collapse = ",")
  })
  
  # Split by grouping keys
  meta_split <- split(table_meta, group_keys)
  
  code_strings <- lapply(names(meta_split), function(group_key) {
    group_meta <- meta_split[[group_key]]
    
    group_vars <- group_meta$grouping_variable[[1]]
  aggregation_expressions <- mapply(
      FUN = function(name, fn, expr) {
        final_fn <- switch(tolower(fn),
                           "count" = ".N",
                           "sum" = "sum",
                           "mean" = "mean",
                           "median" = "median",
                           "min" = "min",
                           "max" = "max",
                           fn
        )
        
        if (final_fn == ".N") {
          sprintf("%s = .N", name)
        } else {
          sprintf("%s = %s(%s)", name, final_fn, expr)
        }
      },
      name = group_meta$aggregated_name,
      fn   = group_meta$aggregation_function,
      expr = group_meta$value_expression,
      SIMPLIFY = TRUE, USE.NAMES = FALSE
    )

    
    j_clause <- paste(aggregation_expressions, collapse = ", ")
    by_clause <- paste0(".(", paste(group_vars, collapse = ", "), ")")
    
    sprintf("%s[, .(%s), by = %s]",
            table_name_filter,
            j_clause,
            by_clause)
  })
  
  names(code_strings) <- names(meta_split)
  unlist(code_strings)
}