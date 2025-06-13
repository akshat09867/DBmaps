library(testthat)

test_that("table_info creates a data.table with correct structure and values", {
  specs <- list(
    list( # Outcome 1
      OutcomeName = "Revenue",
      ValueExpression = quote(price * quantity),
      AggregationMethods = list(
        list(AggregatedName = "TotalRevenueByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id")
      )
    ),
    list( # Outcome 2
      OutcomeName = "UnitsSold",
      ValueExpression = quote(quantity),
      AggregationMethods = list(
        list(AggregatedName = "TotalUnitsByProduct", AggregationFunction = "sum", GroupingVariables = c("product_id", "category_id"))
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
  expect_s3_class(info_dt, "data.frame") # data.table also inherits from data.frame

  # 2. Check column names
  expected_cols <- c("table_name", "source_identifier", "identifier_columns",
                     "outcome_name", "value_expression",
                     "aggregated_name", "aggregation_function", "grouping_variables")
  expect_named(info_dt, expected_cols, ignore.order = FALSE) # Order matters for this set

  # 3. Check number of rows (1 outcome with 1 methods + 1 outcome with 1 method = 2 rows)
  expect_equal(nrow(info_dt), 2)

  # 4. Check values for a few representative fields
  expect_equal(unique(info_dt$table_name), "transactions")
  expect_equal(unique(info_dt$source_identifier), "transactions.csv")
  expect_equal(unique(info_dt$identifier_columns), "tx_id_part1,tx_id_part2") # Check comma separation

  expect_equal(info_dt$outcome_name[1], "Revenue")
  expect_equal(info_dt$value_expression[1], "price * quantity") # deparse output
  expect_equal(info_dt$aggregated_name[1], "TotalRevenueByCustomer")
  expect_equal(info_dt$aggregation_func[1], "sum")
  expect_equal(info_dt$grouping_variables[1], "customer_id")

  expect_equal(info_dt$grouping_variables[2], "product_id,category_id")
  expect_equal(info_dt$outcome_name[2], "UnitsSold")
  expect_equal(info_dt$value_expression[2], "quantity")
})

test_that("table_info handles empty key_outcome_specs correctly", {
  info_dt_empty <- table_info(
    table_name = "empty_table",
    source_identifier = "empty.csv",
    identifier_columns = "id",
    key_outcome_specs = list() # Empty list
  )

  expect_s3_class(info_dt_empty, "data.table")
  expect_equal(nrow(info_dt_empty), 0)
  expected_cols <- c("table_name", "source_identifier", "identifier_columns",
                     "outcome_name", "value_expression",
                     "aggregated_name", "aggregation_function", "grouping_variables")
  expect_named(info_dt_empty, expected_cols, ignore.order = FALSE)
})

test_that("table_info input validation works", {
  # Test for invalid 'table_name'
  expect_error(
    table_info(table_name = 123, source_identifier = "test.csv", identifier_columns = "id", key_outcome_specs = list()),
    "'table_name' must be a single character string."
  )

  # Test for invalid 'identifier_columns' (should be character vector)
  expect_error(
    table_info(table_name = "test", source_identifier = "test.csv", identifier_columns = list("id"), key_outcome_specs = list()),
    "'identifier_columns' must be a character vector"
  )
  expect_error(
    table_info(table_name = "test", source_identifier = "test.csv", identifier_columns = character(0), key_outcome_specs = list()),
    "'identifier_columns' must be a character vector with at least one column name."
  )

  # Test for invalid 'key_outcome_specs' (should be a list)
  expect_error(
    table_info(table_name = "test", source_identifier = "test.csv", identifier_columns = "id", key_outcome_specs = "not a list"),
    "'key_outcome_specs' must be a list."
  )

  test_that("table_info errors when outcome spec is missing ValueExpression", {
  bad_specs <- list(
    list(
      OutcomeName = "InvalidOutcome",
      AggregationMethods = list(
        list(AggregatedName = "Agg", AggregationFunction = "sum", GroupingVariables = "grp")
      )
    )
  )
  expect_error(
    table_info("test", "test.csv", "id", bad_specs),
    "'ValueExpression' must be provided"
  )
})


test_that("table_info errors when outcome spec has no AggregationMethods", {
  bad_specs <- list(
    list(
      OutcomeName = "InvalidOutcome",
      ValueExpression = quote(x + y),
      AggregationMethods = NULL
    )
  )
  expect_error(
    table_info("test", "test.csv", "id", bad_specs),
    "'AggregationMethods' must be a non-empty list"
  )
})

test_that("table_info errors for malformed AggregationMethod", {
  bad_specs <- list(
    list(
      OutcomeName = "Something",
      ValueExpression = quote(a + b),
      AggregationMethods = list(
        list(AggregationFunction = "mean", GroupingVariables = "group_id")
      )
    )
  )
  expect_error(
    table_info("test", "test.csv", "id", bad_specs),
    "'AggregatedName' must be provided"
  )
})

test_that("table_info errors when aggregation spec is NA GroupingVariables", {
  bad_specs <- list(
    list(
      OutcomeName = "MissingGroup",
      ValueExpression = quote(x + y),
      AggregationMethods = list(
        list(AggregatedName = "SumX", AggregationFunction = "sum", GroupingVariable = NA_character_)
      )
    )
  )

  expect_error(
    table_info(
      table_name = "test",
      source_identifier = "test.csv",
      identifier_columns = "id",
      key_outcome_specs = bad_specs
    ),
    "'Grouping Variable' must not be NA"
  )
})

})