library(data.table)



customers_entry <- table_info(
  table_name = "customers",
  source_identifier = "customers.csv",
  identifier_columns = "customer_id",
  key_outcome_specs = list(
    list(OutcomeName = "CustomerCount", ValueExpression = 1, AggregationMethods = list(
      list(AggregatedName = "CountByRegion", AggregationFunction = "sum", GroupingVariables = "region")
    ))
  )
)

products_entry <- table_info(
  table_name = "products",
  source_identifier = "products.csv",
  identifier_columns = "product_id",
  key_outcome_specs = list(
    list(OutcomeName = "ProductCount", ValueExpression = 1, AggregationMethods = list(
      list(AggregatedName = "CountByCategory", AggregationFunction = "sum", GroupingVariables = "category")
    ))
  )
)




transactions_entry <- table_info(
  table_name = "transactions",
  source_identifier = "transactions.csv",
  identifier_columns = c("customer_id", "product_id", "time"),
  key_outcome_specs = list(
    list(
      OutcomeName = "Revenue", ValueExpression = quote(price * quantity),
      AggregationMethods = list(
        list(AggregatedName = "RevenueByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id"),
        list(AggregatedName = "RevenueByProduct", AggregationFunction = "sum", GroupingVariables = "product_id"),
        list(AggregatedName = "DailyRevenueByCustomerProduct", AggregationFunction = "sum",
             GroupingVariables = c("customer_id", "product_id", "time"))
      )
    )
  )
)

views_entry <- table_info(
  table_name = "views",
  source_identifier = "views.csv",
  identifier_columns = c("customer_id", "product_id", "time"),
  key_outcome_specs = list(
    list(
      OutcomeName = "ViewCount", ValueExpression = 1,
      AggregationMethods = list(
        list(AggregatedName = "ViewsByCustomer", AggregationFunction = "count", GroupingVariables = "customer_id"),
        list(AggregatedName = "ViewsByProduct", AggregationFunction = "count", GroupingVariables = "product_id"),
        list(AggregatedName = "DailyViewsByCustomerProduct", AggregationFunction = "sum",
             GroupingVariables = c("customer_id", "product_id", "time"))
      )
    )
  )
)


meta <- MetadataRegistry$new()
meta$add_table(transactions_entry)
meta$add_table(views_entry)
meta$add_table(customers_entry)
meta$add_table(products_entry)
master_metadata_dt <- meta$get_metadata()
print(master_metadata_dt)
