#' Create a Metadata Registry
#'
#' Initializes an empty data.table with a custom class "MetadataRegistry" to
#' store and manage metadata definitions.
#'
#' @return An empty data.table with the class "MetadataRegistry".
#' @export
create_metadata_registry <- function() {
  reg <- data.table()
  class(reg) <- c("MetadataRegistry", "data.table", "data.frame")
  return(reg)
}

#' Add a Table's Metadata to a Registry
#'
#' A generic function to add new table metadata to a registry object.
#'
#' @param registry The registry object to which metadata will be added.
#' @param table_metadata A data.table object created by `table_info()`.
#' @return The updated registry object.
#' @export
add_table <- function(registry, table_metadata) {
  UseMethod("add_table")
}

#' @export
add_table.MetadataRegistry <- function(registry, table_metadata) {
  table_name <- table_metadata$table_name[1]
  message("Added metadata for table: ", table_name)
  
  # rbindlist is used to combine the old registry with the new metadata
  updated_registry <- rbindlist(list(registry, table_metadata), use.names = TRUE, fill = TRUE)
  
  # Ensure the class is retained
  class(updated_registry) <- class(registry)
  return(updated_registry)
}