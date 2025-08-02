library(data.table)

path_customers <- system.file("extdata", "customers.csv", package = "DBmaps")
path_products <- system.file("extdata", "products.csv", package = "DBmaps")
path_transactions <- system.file("extdata", "transactions.csv", package = "DBmaps")
path_views <- system.file("extdata", "views.csv", package = "DBmaps")


customers <- fread(path_customers)
products <- fread(path_products)
transactions <- fread(path_transactions)
views <- fread(path_views)

transactions[, time := as.POSIXct(time, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")]
views[, time := as.POSIXct(time, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")]

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
        list(AggregatedName = "ViewsByProduct", AggregationFunction = "count", GroupingVariables = "product_id"),
        list(AggregatedName = "ViewsByCustomer", AggregationFunction = "count", GroupingVariables = "customer_id")
        ))
  )
)

meta <- create_metadata_registry()
meta <- add_table(meta, customers_info)
meta <- add_table(meta, products_info)
meta <- add_table(meta, transactions_info)
meta <- add_table(meta, views_info)
all_tables <- list(
  customers    = customers,
  products     = products,
  transactions = transactions,
  views = views
)

paths <- map_join_paths(meta, all_tables)

plan <- create_join_plan(
  "products", selections = list(
    products = c("product_id", "category"),
    transactions = "RevenueByProduct",
    views = "ViewsByProduct"
),
metadata_dt = meta,
join_map = paths
)

final_dt <- execute_join_plan(plan, all_tables)
print(final_dt)