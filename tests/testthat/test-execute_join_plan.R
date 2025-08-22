
library(testthat)
library(data.table)


customers_data <- data.table(
  customer_id = c("c1", "c2", "c3"), 
  region = c("NA", "EU", "NA")
)
transactions_data <- data.table(
  transaction_id = 1:4,
  customer_id = c("c1", "c2", "c1", "c3"),
  price = c(10, 20, 15, 5),
  quantity = c(1, 2, 1, 3)
)
views_data <- data.table(
  view_id = 1:4,
  customer_id = c("c1", "c2", "c3", "c1")
)

data_list_complete <- list(
  customers = customers_data,
  transactions = transactions_data,
  views = views_data
)

customers_meta <- table_info("customers", "c.csv", "customer_id", list(list(OutcomeName="x",ValueExpression=1,AggregationMethods=list(list(AggregatedName="y",AggregationFunction="z",GroupingVariables="region")))))
transactions_meta <- table_info(
  "transactions", "t.csv", "tx_id",
  key_outcome_specs = list(list(OutcomeName = "Revenue", ValueExpression = quote(price * quantity),
  AggregationMethods = list(list(AggregatedName = "RevenueByCustomer",
  AggregationFunction = "sum", GroupingVariables = "customer_id"))))
)
views_meta <- table_info(
  "views", "v.csv", "view_id",
  key_outcome_specs = list(list(OutcomeName = "ViewCount", ValueExpression = 1,
  AggregationMethods = list(list(AggregatedName = "ViewsByCustomer",
  AggregationFunction = ".N", GroupingVariables = "customer_id"))))
)
master_metadata <- rbindlist(list(customers_meta, transactions_meta, views_meta))



test_that("executes a simple aggregation and merge plan correctly", {
  # 1. Create a plan for a simple join
  plan <- create_join_plan(
    base_table = "customers",
    selections = list(
      customers = c("customer_id", "region"),
      transactions = "RevenueByCustomer"
    ),
    metadata_dt = master_metadata
  )
  

  result_dt <- suppressMessages(
    execute_join_plan(plan, data_list_complete)
  )
  
  expected_dt <- data.table(
    customer_id = c("c1", "c2", "c3"),
    region = c("NA", "EU", "NA"),
    RevenueByCustomer = c(25, 40, 15) # c1: 10*1 + 15*1 = 25; c2: 20*2 = 40; c3: 5*3=15
  )
  setkey(expected_dt, customer_id)
  setkey(result_dt, customer_id)
  
  expect_s3_class(result_dt, "data.table")
  expect_equal(nrow(result_dt), 3)
  expect_named(result_dt, c("customer_id", "region", "RevenueByCustomer"))
  expect_equal(result_dt, expected_dt)
})

test_that("executes a multi-merge plan correctly", {
  plan <- create_join_plan(
    base_table = "customers",
    selections = list(
      customers = c("customer_id"),
      transactions = "RevenueByCustomer",
      views = "ViewsByCustomer"
    ),
    metadata_dt = master_metadata
  )
  
  result_dt <- suppressMessages(
    execute_join_plan(plan, data_list_complete)
  )
  
  expected_dt <- data.table(
    customer_id = c("c1", "c2", "c3"),
    RevenueByCustomer = c(25, 40, 15),
    ViewsByCustomer = c(2L, 1L, 1L) # c1 has 2 views, c2 and c3 have 1
  )
  setkey(expected_dt, customer_id)
  setkey(result_dt, customer_id)
  
  expect_equal(nrow(result_dt), 3)
  expect_named(result_dt, c("customer_id", "RevenueByCustomer", "ViewsByCustomer"))
  expect_equal(result_dt, expected_dt)
})


test_that("execution is isolated and does not affect the global environment", {
  rm(list = ls(pattern = "^agg_|^merged_|^final_data"), envir = .GlobalEnv)
  expect_false(exists("agg_transactions", envir = .GlobalEnv))
  expect_false(exists("merged_step_2", envir = .GlobalEnv))
  expect_false(exists("final_data", envir = .GlobalEnv))
  
  plan <- create_join_plan(
    base_table = "customers",
    selections = list(customers = "region", transactions = "RevenueByCustomer"),
    metadata_dt = master_metadata
  )
  result <- suppressMessages(
    execute_join_plan(plan, data_list_complete)
  )
  
  expect_false(exists("agg_transactions", envir = .GlobalEnv))
  expect_false(exists("merged_step_2", envir = .GlobalEnv))
  expect_false(exists("final_data", envir = .GlobalEnv))
  
  expect_s3_class(result, "data.table")
})

test_that("fails with a clear error if a source data table is missing", {
  plan <- create_join_plan(
    base_table = "customers",
    selections = list(customers = "region", transactions = "RevenueByCustomer"),
    metadata_dt = master_metadata
  )
  
  data_list_incomplete <- list(
    customers = customers_data
  )
  
  expect_error(
    execute_join_plan(plan, data_list_incomplete),
    "object 'transactions' not found"
  )
})