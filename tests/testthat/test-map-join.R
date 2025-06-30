library(testthat)
library(data.table)

test_metadata <- rbindlist(list(
  # 1. Customers dimension table
  table_info(
    table_name = "customers",
    source_identifier = "customers.csv",
    identifier_columns = "customer_id",
    key_outcome_specs = list(
      list(OutcomeName = "Count", ValueExpression = 1, AggregationMethods = list(
        list(AggregatedName = "CountByRegion", AggregationFunction = "sum", GroupingVariables = "region")
      ))
    )
  ),
  # 2. Products dimension table
  table_info(
    table_name = "products",
    source_identifier = "products.csv",
    identifier_columns = "product_id",
    key_outcome_specs = list(list(OutcomeName = "ProductCount", ValueExpression = 1,
  AggregationMethods = list(list(AggregatedName = "ProductsPerCategory",
  AggregationFunction = "sum", GroupingVariables = "category"))))
  ),
  # 3. Transactions fact table
  table_info(
    table_name = "transactions",
    source_identifier = "transactions.csv",
    identifier_columns = "tx_id",
    key_outcome_specs = list(
      list(OutcomeName = "Revenue", ValueExpression = quote(price * quant), AggregationMethods = list(
        list(AggregatedName = "RevenueByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id"),
        list(AggregatedName = "RevenueByProduct", AggregationFunction = "sum", GroupingVariables = "product_id")
      ))
    )
  ),
  # 4. Views fact table
  table_info(
    table_name = "views",
    source_identifier = "views.csv",
    identifier_columns = "view_id",
    key_outcome_specs = list(
      list(OutcomeName = "ViewCount", ValueExpression = 1, AggregationMethods = list(
        list(AggregatedName = "ViewsByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id")
      ))
    )
  )
))
test_that("map_join_paths correctly identifies all valid paths", {
  join_map <- map_join_paths(test_metadata)
  # Expected paths:
  # 1. transactions -> customers (on customer_id)
  # 2. transactions -> products (on product_id)
  # 3. views -> customers (on customer_id)
  expected_map <- data.table(
    table_from = c("transactions", "transactions", "views"),
    table_to   = c("customers", "products", "customers"),
    join_key   = c("customer_id", "product_id", "customer_id")
  )

  # Check object type and dimensions
  expect_s3_class(join_map, "data.table")
  expect_equal(nrow(join_map), 3)
  expect_named(join_map, c("table_from", "table_to", "join_key"))

  # Sort both tables by all columns to ensure comparison is not order-dependent
  setorder(join_map, table_from, table_to, join_key)
  setorder(expected_map, table_from, table_to, join_key)

  # Check that the content is identical
  expect_equal(join_map, expected_map)
})

test_that("map_join_paths does not create paths for non-matching grouping variables", {
  # The test_metadata for `customers` has a `grouping_vars` of "region",
  # which does not match any table's primary key. Therefore, no path should
  # originate from the `customers` table.
  join_map <- map_join_paths(test_metadata)

  expect_false("customers" %in% join_map$table_from)
})

test_that("map_join_paths handles no possible paths gracefully", {
  # Create metadata where no keys match
  no_path_metadata <- rbindlist(list(
    table_info("table_a", "a.csv", "a_id", list(list(
      OutcomeName = "O", ValueExpression = 1, AggregationMethods = list(
        list(AggregatedName = "A", AggregationFunction = "sum", GroupingVariables = "group_a")
      )))),
    table_info("table_b", "b.csv", "b_id", list(list(
      OutcomeName = "s", ValueExpression = 1, AggregationMethods = list(
        list(AggregatedName = "B", AggregationFunction = "sum", GroupingVariables = "group_b")
      ))))
  ))

  # Expect a warning and an empty data.table with the correct structure
  expect_warning(
    result <- map_join_paths(no_path_metadata),
    "No potential join paths were found"
  )

  expect_s3_class(result, "data.table")
  expect_equal(nrow(result), 0)
  expect_named(result, c("table_from", "table_to", "join_key"))
})

test_that("map_join_paths errors on incorrect input type", {
  # Input must be a data.table, not a list or data.frame
  expect_error(
    map_join_paths(list()),
    "'metadata_dt' must be a data.table."
  )
  expect_error(
    map_join_paths(as.data.frame(test_metadata)),
    "'metadata_dt' must be a data.table."
  )
})