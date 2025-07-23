#' Create a Plan for Aggregating and Merging Tables
#'
#' This function acts as a "planner." It takes a user's request for a final
#' dataset, finds a path using a join map, and creates a structured plan
#' (or "recipe") of the necessary steps.
#'
#' @param base_table A character string specifying the main table.
#' @param selections A named list specifying the columns or aggregations to include.
#' @param metadata_dt The master metadata data.table.
#' @param join_map An optional "Join Map" data.table produced by `map_join_paths()`.
#'   If `NULL` (the default), the map will be generated automatically from the metadata.
#' @param tables_dis An optional named list of data.tables used for data‑driven (inferred) join discovery. If `NULL`, only metadata‑driven joins are used.
#'   If `NULL` (the default), the map will be generated automatically from the metadata.
#' @return A list object representing the "join plan."
#' @importFrom data.table is.data.table
#' @export
#' @examples
#' # --- 1. Define Metadata (Prerequisite) ---
#' customers_meta <- table_info(
#'  table_name = "customers",
#'  source_identifier = "customers.csv",
#'  identifier_columns = "customer_id",
#'  key_outcome_specs = list(
#'    list(OutcomeName = "CustomerCount", ValueExpression = 1, AggregationMethods = list(
#'      list(AggregatedName = "CountByRegion", AggregationFunction = "sum", GroupingVariables = "region")
#'    ))
#'  )
#')
#' transactions_meta <- table_info(
#'   "transactions", "t.csv", "tx_id",
#'   key_outcome_specs = list(list(OutcomeName = "Revenue", ValueExpression = quote(r),
#'   AggregationMethods = list(list(AggregatedName = "RevenueByCustomer",
#'   AggregationFunction = "sum", GroupingVariables = "customer_id"))))
#' )
#' master_metadata <- data.table::rbindlist(list(customers_meta, transactions_meta))
#'
#' # --- 2. Define the Desired Output ---
#' user_selections <- list(
#'   customers = "region",
#'   transactions = "RevenueByCustomer"
#' )
#'
#' # --- 3. Create the Join Plan WITHOUT providing the join_map ---
#' # The function will now generate it automatically.
#' join_plan <- create_join_plan(
#'   base_table = "customers",
#'   selections = user_selections,
#'   metadata_dt = master_metadata
#' )
#'
#' # --- 4. Inspect the Plan ---
#' str(join_plan)
#'
create_join_plan <- function(base_table, selections, metadata_dt, join_map = NULL, tables_dis = NULL) {

  # --- Input Validation ---
  if (!is.character(base_table) || length(base_table) != 1) stop("'base_table' must be a single character string.")
  if (!is.list(selections) || is.null(names(selections))) stop("'selections' must be a named list.")
  if (!data.table::is.data.table(metadata_dt)) stop("'metadata_dt' must be a data.table.")

  if (is.null(join_map)) {
    message("Join map not provided. Generating from metadata...")
    join_map <- map_join_paths(metadata_dt, tables_dis)
  }

  if (!data.table::is.data.table(join_map)) stop("'join_map' must be a data.table.")

  # --- Initialization ---
  internal_plan <- list(
    aggregations = list(),
    merges = list(),
    final_cols = list()
  )
  
  target_tables <- names(selections)
  internal_plan$final_cols[[base_table]] <- selections[[base_table]]

  # --- Path Finding & Planning ---
  for (tbl_name in setdiff(target_tables, base_table)) {
    path_to_base <- join_map[table_from == tbl_name & table_to == base_table]
    
    if (nrow(path_to_base) == 0) {
      warning("No direct path found from '", tbl_name, "' to '", base_table, "'. Skipping this table.'")
      next
    }
    
    path <- path_to_base[1, ]
    join_key <- path$key_to[[1]]
    selected_aggs <- selections[[tbl_name]]
    agg_metadata <- metadata_dt[table_name == tbl_name & aggregated_name %in% selected_aggs]
    
    if (nrow(agg_metadata) > 0) {
      agg_codes <- generate_aggregation_code(tbl_name, agg_metadata)
      
      # Find matching aggregation by grouping variables
      found_match <- FALSE
      for (i in seq_along(agg_codes)) {
        grp_vars <- names(agg_codes)[i]
        if (setequal(strsplit(grp_vars, ",")[[1]], join_key)) {
          agg_code <- agg_codes[[i]]
          found_match <- TRUE
          break
        }
      }
      
      if (!found_match) {
        stop(sprintf(
          "No aggregation found for table '%s' matching join key: %s",
          tbl_name, paste(join_key, collapse = ", ")
        ))
      }

      agg_table_name <- paste0("agg_", tbl_name)
      
      internal_plan$aggregations[[agg_table_name]] <- list(
        source_table = tbl_name,
        new_name = agg_table_name,
        code = agg_code
      )
      
      internal_plan$merges[[agg_table_name]] <- list(
        left_table = base_table,
        right_table = agg_table_name,
        by = join_key
      )
      
      internal_plan$final_cols[[agg_table_name]] <- agg_metadata$aggregated_name
    }
  }

  flat_plan_steps <- list()
  step_counter <- 1
  
  # 1. Add aggregation steps to the flat plan
  for (agg_step in internal_plan$aggregations) {
    flat_plan_steps[[step_counter]] <- data.table(
      step = step_counter,
      operation = "AGGREGATE",
      target = agg_step$new_name,
      details = paste("Aggregate", sQuote(agg_step$source_table)),
      code = agg_step$code
    )
    step_counter <- step_counter + 1
  }

  # 2. Add merge steps to the flat plan
  left_tbl <- base_table
  for (merge_step in internal_plan$merges) {
    right_tbl <- merge_step$right_table
    by_cols_str <- paste0("c('", paste(merge_step$by, collapse = "','"), "')")
    
    target_tbl <- paste0("merged_step_", step_counter)
    
    merge_code <- sprintf("%s <- merge(x = %s, y = %s, by = %s, all.x = TRUE)",
                          target_tbl, left_tbl, right_tbl, by_cols_str)
    
    flat_plan_steps[[step_counter]] <- data.table(
      step = step_counter,
      operation = "MERGE",
      target = target_tbl,
      details = paste("Merge", sQuote(left_tbl), "with", sQuote(right_tbl)),
      code = merge_code
    )
    left_tbl <- target_tbl 
    step_counter <- step_counter + 1
  }
  
  # 3. Add a final selection step
  final_cols_vec <- unlist(unique(internal_plan$final_cols, use.names = FALSE))
  final_cols_str <- paste0("c('", paste(final_cols_vec, collapse = "','"), "')")
  
  select_code <- sprintf("final_data <- %s[, .SD, .SDcols = %s]",
                         left_tbl, final_cols_str)

  flat_plan_steps[[step_counter]] <- data.table(
    step = step_counter,
    operation = "SELECT",
    target = "final_data",
    details = "Select final columns",
    code = select_code
  )
  
  if (length(flat_plan_steps) == 0) return(data.table())
  
  return(data.table::rbindlist(flat_plan_steps))
  
  return(plan)
}