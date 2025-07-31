library(testthat)
library(data.table)


customers_meta <- table_info("customers", "c.csv", "customer_id", list(list(OutcomeName="x",ValueExpression=1,AggregationMethods=list(list(AggregatedName="y",AggregationFunction="z",GroupingVariables="region")))))
products_meta <- table_info("products", "p.csv", "product_id", list(list(OutcomeName="x",ValueExpression=1,AggregationMethods=list(list(AggregatedName="y",AggregationFunction="z",GroupingVariables="category")))))
transactions_meta <- table_info("transactions", "t.csv", "trans_id", list(
  list(OutcomeName="Revenue", ValueExpression=quote(price*qty), AggregationMethods=list(
    list(AggregatedName="RevenueByCustomer", AggregationFunction="sum", GroupingVariables="customer_id"),
    list(AggregatedName="RevenueByProduct", AggregationFunction="sum", GroupingVariables="product_id")
  ))
))
views_meta <- table_info("views", "v.csv", "view_id", list(
  list(OutcomeName="ViewCount", ValueExpression=1, AggregationMethods=list(
    list(AggregatedName="ViewsByCustomer", AggregationFunction="count", GroupingVariables="customer_id")
  ))
))

master_metadata <- create_metadata_registry()
master_metadata <- add_table(master_metadata, customers_meta)
master_metadata <- add_table(master_metadata, products_meta)
master_metadata <- add_table(master_metadata, views_meta)
master_metadata <- add_table(master_metadata, transactions_meta)
test_join_map <- map_join_paths(master_metadata)


# --- Test Cases ---
test_that("generates a correct plan for a single aggregation and merge", {
  plan <- create_join_plan(
    base_table = "customers",
    selections = list(customers = "region", transactions = "RevenueByCustomer"),
    metadata_dt = master_metadata,
    join_map = test_join_map
  )
  
  expect_equal(nrow(plan), 3)
  expect_equal(plan$operation, c("AGGREGATE", "MERGE", "SELECT"))
  expect_true(grepl("transactions", plan$code[1]))
  expect_true(grepl("agg_transactions", plan$code[2]))
  expect_true(grepl("merged_step_2", plan$code[3]))
})

test_that("generates a correct plan for multiple merges", {
  plan <- create_join_plan(
    base_table = "customers",
    selections = list(
      customers = "region", 
      transactions = "RevenueByCustomer", 
      views = "ViewsByCustomer"
    ),
    metadata_dt = master_metadata,
    join_map = test_join_map
  )
  
  expect_equal(nrow(plan), 5)
  expect_equal(plan$operation, c("AGGREGATE", "AGGREGATE", "MERGE", "MERGE", "SELECT")[1:5])
  expect_equal(plan$target[1], "agg_transactions")
  expect_equal(plan$target[2], "agg_views")
  expect_true(grepl("merged_step_4", plan$code[4]))
})

test_that("stops with an informative error for mismatched keys", {
  # Requesting an aggregation (by product_id) that cannot join to the base table (customers)
  selections <- list(
    customers = "customer_id",
    transactions = "RevenueByProduct" # This is grouped by product_id
  )
  
  # The error message should clearly state the problem
  expected_error_msg <- "No aggregation found for table 'transactions' matching join key: customer_id"
  
  expect_error(
    create_join_plan(
      base_table = "customers",
      selections = selections,
      metadata_dt = master_metadata,
      join_map = test_join_map
    ),
    expected_error_msg,
    fixed = TRUE
  )
})

test_that("handles requests with no aggregations needed", {
  # Requesting only columns from the base table
  plan <- create_join_plan(
    base_table = "customers",
    selections = list(customers = c("customer_id", "region")),
    metadata_dt = master_metadata,
    join_map = test_join_map
  )
  
  expect_equal(nrow(plan), 1)
  expect_equal(plan$operation, "SELECT")
  expect_true(grepl("customers[, .SD, .SDcols", plan$code, fixed = TRUE))
})