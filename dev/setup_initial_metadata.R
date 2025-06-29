transactions_entry <- table_info(
  table_name = "transactions",
  source_identifier = "transactions.csv", 
  identifier_columns = c("customer_id", "product_id", "time"),
  key_outcome_specs = list(
    list(
      OutcomeName = "Revenue",
      ValueExpression = quote(price * quantity),
      AggregationMethods = list(
        list(AggregatedName = "TotalRevenueByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id"),
        list(AggregatedName = "TotalRevenueByProduct",  AggregationFunction = "sum", GroupingVariables = "product_id")
      )
    ),
    list(
      OutcomeName = "TransactionCount",
      ValueExpression = 1,
      AggregationMethods = list(
        list(AggregatedName = "TransactionsByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id")
      )
    ),
    list(
      OutcomeName = "UnitsSold",
      ValueExpression = quote(quantity),
      AggregationMethods = list(
        list(AggregatedName = "UnitsSoldByProduct", AggregationFunction = "sum", GroupingVariables = "product_id")
      )
    )
  )
)


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

product_entry <- table_info(
  table_name = "products",
  source_identifier = "products.csv",
  identifier_columns = "product_id",
  key_outcome_specs = list(
    list(OutcomeName = "ProductCount", ValueExpression = 1, AggregationMethods = list(
      list(AggregatedName = "ProductsPerCategory", AggregationFunction = "sum", GroupingVariables = "category")
    ))
  )
)



views_entry <- table_info(
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

mater_metadata_dt <- rbindlist(list(customers_entry, product_entry, transactions_entry, views_entry))

print(mater_metadata_dt)

jon <- map_join_paths(mater_metadata_dt)
print(jon)