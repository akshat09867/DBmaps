library(testthat)


test_metadata <- rbindlist(list(
  # Table 1: transactions, with multiple outcomes and grouping sets
  table_info(
    table_name = "transactions",
    source_identifier = "transactions.csv",
    identifier_columns = "tx_id",
    key_outcome_specs = list(
      list(OutcomeName = "Revenue", ValueExpression = quote(price * quant), AggregationMethods = list(
        list(AggregatedName = "RevenueByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id")
      )),
      list(OutcomeName = "UnitsSold", ValueExpression = quote(quant), AggregationMethods = list(
        list(AggregatedName = "UnitsByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id")
      )),
      list(OutcomeName = "Revenue", ValueExpression = quote(price * quant), AggregationMethods = list(
        list(AggregatedName = "RevenueByProduct", AggregationFunction = "sum", GroupingVariables = "product_id")
      ))
    )
  ),
  # Table 2: customers, with a single aggregation rule
  table_info(
    table_name = "customers",
    source_identifier = "customers.csv",
    identifier_columns = "customer_id",
    key_outcome_specs = list(
      list(OutcomeName = "CustomerCount", ValueExpression = 1, AggregationMethods = list(
        list(AggregatedName = "CountByRegion", AggregationFunction = "sum", GroupingVariables = "region")
      ))
    )
  )
))


test_that("generate_aggregation_code handles multiple aggregations with the same group key", {
  # For transactions table, there are two aggregations by 'customer_id'
  code <- generate_aggregation_code("transactions", test_metadata)

  customer_code <- code["customer_id"]
expected_customer_code <- c(
  customer_id = 
    "transactions[, .(RevenueByCustomer = sum(price * quant), UnitsByCustomer = sum(quant)), by = .(customer_id)]"
)

  expect_named(code, c("customer_id", "product_id"))
  expect_length(code, 2)
  expect_equal(customer_code, expected_customer_code)
})

test_that("generate_aggregation_code handles aggregations with different group keys", {
  # The "transactions" table has aggregations grouped by 'customer_id' and 'product_id'
  code <- generate_aggregation_code("transactions", test_metadata)

  # Expect two separate code strings, one for each group key
  expect_length(code, 2)
  expect_true("customer_id" %in% names(code))
  expect_true("product_id" %in% names(code))

  # Check the product_id aggregation string
  product_code <- code["product_id"]
  expected_product_code <- c(product_id = "transactions[, .(RevenueByProduct = sum(price * quant)), by = .(product_id)]")
  expect_equal(product_code, expected_product_code)
})


test_that("generate_aggregation_code works for a simple case", {
  # The "customers" table has only one aggregation rule
  code <- generate_aggregation_code("customers", test_metadata)

  expect_length(code, 1)
  expect_named(code, "region")
  expected_code <- c(region = "customers[, .(CountByRegion = sum(1)), by = .(region)]")
  expect_equal(code["region"], expected_code)
})


test_that("generate_aggregation_code handles non-existent tables gracefully", {
  # Requesting a table not in the metadata should return an empty vector and a warning
  expect_warning(
    code <- generate_aggregation_code("non_existent_table", test_metadata),
    "No metadata found for table: non_existent_table"
  )
  expect_equal(length(code), 0)
  expect_true(is.character(code))
})
