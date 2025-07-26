#' Discover Potential Join Paths from Metadata and Data
#'
#' Analyzes metadata for explicit joins and optionally scans data to infer
#' additional joins. Handles single- and multi-variable join keys.
#'
#' @param metadata_dt A data.table containing the master metadata.
#' @param data_list A named list of data.tables (names match `table_name` in `metadata_dt`). 
#'        If provided, scans data to find inferred join paths. Defaults to `NULL`.
#' @return A `data.table` representing the "Join Map" with columns:
#'         `table_from`, `table_to`, `key_from`, `key_to`
#' @importFrom data.table rbindlist is.data.table data.table
#' @export
map_join_paths <- function(metadata_dt, data_list = NULL) {

  if (!data.table::is.data.table(metadata_dt)) {
    stop("'metadata_dt' must be a data.table.")
  }

  create_key_string <- function(keys) {
    if (is.list(keys)) keys <- keys[[1]]
    paste(sort(unique(keys)), collapse = ",")
  }

  all_paths <- list()

  pks_raw <- metadata_dt[, .(table_name, identifier_columns)]
  pks_raw[, temp_key_string := sapply(identifier_columns, create_key_string)]
  all_pks <- unique(pks_raw, by = c("table_name", "temp_key_string"))
  all_pks[, temp_key_string := NULL]

  grouping_keys_raw <- metadata_dt[, .(table_name, grouping_variable)]
  grouping_keys_raw[, temp_key_string := sapply(grouping_variable, create_key_string)]
  all_grouping_keys <- unique(grouping_keys_raw, by = c("table_name", "temp_key_string"))
  all_grouping_keys[, temp_key_string := NULL]

  for (i in 1:nrow(all_grouping_keys)) {
    from_table_name <- all_grouping_keys[i, table_name]
    key_from <- all_grouping_keys[i, grouping_variable][[1]]

    for (j in 1:nrow(all_pks)) {
      to_table_name <- all_pks[j, table_name]
      key_to <- all_pks[j, identifier_columns][[1]]

      if (from_table_name == to_table_name) next

      if (isTRUE(all.equal(sort(key_from), sort(key_to)))) {
        all_paths[[length(all_paths) + 1]] <- data.table(
          table_from = from_table_name,
          table_to = to_table_name,
          key_from = list(key_from),
          key_to = list(key_to)
        )
      }
    }
  }

  if (!is.null(data_list)) {
    if (!is.list(data_list) || is.null(names(data_list))) {
      stop("'data_list' must be a named list of data.tables.")
    }
    if (!all(sapply(data_list, data.table::is.data.table))) {
      stop("All elements in 'data_list' must be data.tables.")
    }

    pk_candidates <- list()
    for (tbl_name in names(data_list)) {
      dt <- data_list[[tbl_name]]
      for (col_name in names(dt)) {
        if (anyDuplicated(dt[[col_name]]) == 0) {  
          pk_candidates[[length(pk_candidates) + 1]] <- list(
            table = tbl_name,
            column = col_name,
            type = class(dt[[col_name]])[1],
            values = dt[[col_name]]
          )
        }
      }
    }
    
    for (from_tbl_name in names(data_list)) {
      from_dt <- data_list[[from_tbl_name]]
      for (from_col_name in names(from_dt)) {
        fk_values <- unique(from_dt[[from_col_name]])
        fk_type <- class(from_dt[[from_col_name]])[1]
        
        for (pk in pk_candidates) {
          if (from_tbl_name == pk$table) next  
          if (fk_type != pk$type) next  
          
          if (all(fk_values %in% pk$values)) {
            all_paths[[length(all_paths) + 1]] <- data.table(
              table_from = from_tbl_name,
              table_to = pk$table,
              key_from = list(from_col_name),
              key_to = list(pk$column)
            )
          }
        }
      }
    }
  }

  if (length(all_paths) == 0) {
    warning("No potential join paths were found.")
    return(data.table(
      table_from = character(),
      table_to = character(),
      key_from = list(),
      key_to = list()
    ))
  }
  
  join_map_raw <- rbindlist(all_paths)

  join_map_raw[, temp_join_key := paste(
    pmin(table_from, table_to), 
    pmax(table_from, table_to),
    sapply(key_from, create_key_string),
    sapply(key_to, create_key_string)
  )]

  final_map <- unique(join_map_raw, by = "temp_join_key")
  final_map[, temp_join_key := NULL]

  return(final_map)
}