library(data.table)

path_customers <- system.file("extdata", "customers.csv", package = "DBmaps")
path_products <- system.file("extdata", "products.csv", package = "DBmaps")
path_transactions <- system.file("extdata", "transactions.csv", package = "DBmaps")
path_views <- system.file("extdata", "views.csv", package = "DBmaps")


customers_dt <- fread(path_customers)
products_dt <- fread(path_products)
transactions_dt <- fread(path_transactions)
views_dt <- fread(path_views)

transactions_dt[, time := as.POSIXct(time, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")]
views_dt[, time := as.POSIXct(time, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")]

cat("--- All 4 Raw Data Tables Loaded Successfully ---\n\n")

customers_info <- table_info(
  table_name = "customers",
  source_identifier = "customers.csv",
  identifier_columns = "customer_id",
  key_outcome_specs = list(
    list(OutcomeName = "CustomerCount", ValueExpression = 1, AggregationMethods = list(
      list(AggregatedName = "CountByRegion", AggregationFunction = "sum", GroupingVariables = "region")
    ))
  )
)

products_info <- table_info(
  table_name = "products",
  source_identifier = "products.csv",
  identifier_columns = "product_id",
  key_outcome_specs = list(
    list(OutcomeName = "ProductCount", ValueExpression = 1, AggregationMethods = list(
      list(AggregatedName = "ProductsPerCategory", AggregationFunction = "sum", GroupingVariables = "category")
    ))
  )
)

transactions_info <- table_info(
  table_name = "transactions",
  source_identifier = "transactions.csv",
  identifier_columns = c("customer_id", "product_id", "time"),
  key_outcome_specs = list(
    list(OutcomeName = "Revenue", ValueExpression = quote(price * quantity), AggregationMethods = list(
      list(AggregatedName = "RevenueByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id"),
      list(AggregatedName = "RevenueByProduct", AggregationFunction = "sum", GroupingVariables = "product_id")
    )),
    list(OutcomeName = "UnitsSold", ValueExpression = quote(quantity), AggregationMethods = list(
      list(AggregatedName = "TotalUnitsByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id")
    ))
  )
)

views_info <- table_info(
  table_name = "views",
  source_identifier = "views.csv",
  identifier_columns = c("customer_id", "product_id", "time"),
  key_outcome_specs = list(
    list(OutcomeName = "ViewCount", ValueExpression = 1, AggregationMethods = list(
        list(AggregatedName = "ViewsByProduct", AggregationFunction = "sum", GroupingVariables = "product_id"),
        list(AggregatedName = "ViewsByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id")
        ))
  )
)


master_metadata_dt <- rbindlist(list(customers_info, products_info, transactions_info, views_info))
print(master_metadata_dt)

generated_code <- generate_aggregation_code(table_name_filter = "transactions", metadata_dt = master_metadata_dt)
cat("Generated Code Strings for 'transactions':\n")

code_to_run <- generated_code["customer_id"]

cat("Code to be executed:\n")
cat(code_to_run, "\n\n")

dt_name_string <- "transactions_dt"
code_to_run_on_object <- gsub("^transactions", dt_name_string, code_to_run)

aggregated_transactions_by_customer <- eval(parse(text = code_to_run_on_object))

cat("Result of Execution (Aggregated Data):\n")
print(aggregated_transactions_by_customer)
cat("\n--- Example Complete ---\n")
