library(testthat)
library(data.table)


test_that("table_info creates a data.table with correct list-column structure", {
  specs <- list(
    list(
      OutcomeName = "Revenue",
      ValueExpression = quote(price * quantity),
      AggregationMethods = list(
        list(AggregatedName = "RevenueByCustomer", AggregationFunction = "sum",
             GroupingVariables = "customer_id")
      )
    ),
    list(
      OutcomeName = "UnitsSold",
      ValueExpression = quote(quantity),
      AggregationMethods = list(
        list(AggregatedName = "TotalUnitsByProduct", AggregationFunction = "sum",
             GroupingVariables = c("product_id", "category_id"))
      )
    )
  )

  info_dt <- table_info(
    table_name = "transactions",
    source_identifier = "transactions.csv",
    identifier_columns = c("tx_id_part1", "tx_id_part2"),
    key_outcome_specs = specs
  )

  # 1. Check output type
  expect_s3_class(info_dt, "data.table")

  # 2. Check column names
  expected_cols <- c("table_name", "source_identifier", "identifier_columns", "outcome_name",
                     "value_expression", "aggregated_name", "aggregation_function",
                     "grouping_variable")
  expect_named(info_dt, expected_cols)

  # 3. Check number of rows
  expect_equal(nrow(info_dt), 2)

  # 4. Check values, correctly testing the list-columns
  expect_equal(info_dt$table_name, rep("transactions", 2))
  expect_equal(info_dt$source_identifier, rep("transactions.csv", 2))
  expect_equal(info_dt$identifier_columns, rep(list(c("tx_id_part1", "tx_id_part2")), 2))
  expect_equal(info_dt$outcome_name, c("Revenue", "UnitsSold"))
  expect_equal(info_dt$value_expression, c("price * quantity", "quantity"))
  expect_equal(info_dt$aggregated_name, c("RevenueByCustomer", "TotalUnitsByProduct"))
  expect_equal(info_dt$aggregation_function, rep("sum", 2))
  expect_equal(info_dt$grouping_variable, list("customer_id", c("product_id", "category_id")))
})


# --- Tests for Edge Cases and Validation ---

test_that("table_info handles empty key_outcome_specs correctly", {
  info_dt_empty <- table_info(
    table_name = "empty_table",
    source_identifier = "empty.csv",
    identifier_columns = "id",
    key_outcome_specs = list() # Empty list
  )

  expect_s3_class(info_dt_empty, "data.table")
  expect_equal(nrow(info_dt_empty), 0)
  # Check for the new column names
  expected_cols <- c("table_name", "source_identifier", "identifier_columns", "outcome_name",
                     "value_expression", "aggregated_name", "aggregation_function",
                     "grouping_variable")
  expect_named(info_dt_empty, expected_cols)
})

test_that("table_info validates its main inputs", {
  # Test for invalid 'table_name'
  expect_error(
    table_info(table_name = 123, source_identifier = "test.csv", identifier_columns = "id", key_outcome_specs = list()),
    "'table_name' must be a single character string."
  )

  # Test for invalid 'identifier_columns'
  expect_error(
    table_info(table_name = "test", source_identifier = "test.csv", identifier_columns = list("id"), key_outcome_specs = list()),
    "'identifier_columns' must be a character vector with at least one column name."
  )
  expect_error(
    table_info(table_name = "test", source_identifier = "test.csv", identifier_columns = character(0), key_outcome_specs = list()),
    "'identifier_columns' must be a character vector with at least one column name."
  )
})

test_that("table_info validates the structure of key_outcome_specs", {
  # Test for invalid 'key_outcome_specs' (should be a list)
  expect_error(
    table_info(table_name = "test", source_identifier = "test.csv", identifier_columns = "id", key_outcome_specs = "not a list"),
    "'key_outcome_specs' must be a list."
  )

  # Test for missing ValueExpression
  bad_specs_no_expr <- list(list(OutcomeName = "Invalid", AggregationMethods = list(list(AggregatedName="A", AggregationFunction="sum", GroupingVariables="g"))))
  expect_error(
    table_info("test", "test.csv", "id", bad_specs_no_expr),
    "'ValueExpression' must be provided"
  )
})

test_that("table_info validates AggregationMethods structure", {
  # Test for missing GroupingVariables
  bad_specs_no_group <- list(list(
    OutcomeName = "Revenue", ValueExpression = quote(p*q), AggregationMethods = list(
      list(AggregatedName = "Rev", AggregationFunction = "sum") # Missing GroupingVariables
  )))
  expect_error(
    table_info("test", "test.csv", "id", bad_specs_no_group),
    "Each 'AggregationMethods' entry must have at least one 'GroupingVariable'"
  )

  # Test for empty GroupingVariables
  bad_specs_empty_group <- list(list(
    OutcomeName = "Revenue", ValueExpression = quote(p*q), AggregationMethods = list(
      list(AggregatedName = "Rev", AggregationFunction = "sum", GroupingVariables = character(0))
  )))
  expect_error(
    table_info("test", "test.csv", "id", bad_specs_empty_group),
    "Each 'AggregationMethods' entry must have at least one 'GroupingVariable'"
  )
})