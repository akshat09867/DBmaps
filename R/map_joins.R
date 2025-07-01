#' Discover Potential Join Paths from Metadata
#'
#' Analyzes a master metadata data.table to identify all potential direct join
#' paths between tables. A path is discovered when a table's defined aggregation
#' grouping variable(s) match the primary key(s) of another table.
#'
#' @param metadata_dt A data.table containing the master metadata, created by
#'   calling `table_info()` for multiple tables and combining them.
#' @return A `data.table` representing the "Join Map," with each row defining a
#'   potential join. The table has columns: `table_from`, `table_to`, `join_key`.
#' @importFrom data.table fsetdiff rbindlist
#' @export
#' @examples
#' # --- 1. Create Master Metadata (as we did in the vignettes) ---
#' customers_info <- table_info(
#'   table_name = "customers",
#'   source_identifier = "customers.csv",
#'   identifier_columns = "customer_id",
#'   key_outcome_specs = list(list(OutcomeName = "CustomerCount", ValueExpression = 1,
#'   AggregationMethods = list(list(AggregatedName = "CountByRegion",
#'   AggregationFunction = "sum", GroupingVariables = "region"))))
#' )
#'
#' products_info <- table_info(
#'   table_name = "products",
#'   source_identifier = "products.csv",
#'   identifier_columns = "product_id",
#'   key_outcome_specs = list(list(OutcomeName = "ProductCount", ValueExpression = 1,
#'   AggregationMethods = list(list(AggregatedName = "ProductsPerCategory",
#'   AggregationFunction = "sum", GroupingVariables = "category"))))
#' )
#'
#' transactions_info <- table_info(
#'   table_name = "transactions",
#'   source_identifier = "transactions.csv",
#'   identifier_columns = c("customer_id", "product_id", "time"),
#'   key_outcome_specs = list(
#'     list(OutcomeName = "Revenue", ValueExpression = quote(price * quantity),
#'     AggregationMethods = list(
#'       list(AggregatedName = "RevenueByCustomer", AggregationFunction = "sum",
#'            GroupingVariables = "customer_id"),
#'       list(AggregatedName = "RevenueByProduct", AggregationFunction = "sum",
#'            GroupingVariables = "product_id")
#'     ))
#'   )
#' )
#'
#' master_metadata <- data.table::rbindlist(list(customers_info, products_info, transactions_info))
#'
#' # --- 2. Generate the Join Map from the metadata ---
#' join_map <- map_join_paths(master_metadata)
#'
#' print(join_map)
#'
map_join_paths <- function(metadata_dt) {

  # Ensure input is a data.table
  if (!data.table::is.data.table(metadata_dt)) {
    stop("'metadata_dt' must be a data.table.")
  }

  # Get a unique list of all tables defined in the metadata
  all_tables <- unique(metadata_dt[, .(table_name, identifier_columns)])
  # Create an empty list to store the paths we find
  path_list <- list()

  # Iterate through each row of the metadata. Each row represents a potential
  # aggregated table (the "from" side of our join).
  for (i in 1:nrow(metadata_dt)) {
    from_row <- metadata_dt[i, ]
    from_table_name <- from_row$table_name
    potential_join_key <- from_row$grouping_variables

    # Now, search for a table whose primary key matches this potential join key
    # We also ensure we don't try to join a table to itself.
    possible_to_tables <- all_tables[identifier_columns == potential_join_key & table_name != from_table_name]
    if (nrow(possible_to_tables) > 0) {
      # We found one or more matching tables. Create a path for each.
      for (j in 1:nrow(possible_to_tables)) {
        to_table_name <- possible_to_tables[j, ]$table_name
        path_list[[length(path_list) + 1]] <- data.table::data.table(
          table_from = from_table_name,
          table_to = to_table_name,
          join_key = potential_join_key
        )
      }
    }
  }

  if (length(path_list) == 0) {
    warning("No potential join paths were found in the provided metadata.")
    return(data.table(table_from = character(), table_to = character(), join_key = character()))
  }

  # Combine all found paths into a single data.table and return the unique paths
  join_map <- unique(rbindlist(path_list))

  return(join_map)
}