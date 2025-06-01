transactions_entry <- define_table_meta(
  table_name = "transactions",
  source_identifier = "transactions.csv", 
  description = "Records of individual sales transactions, linking customers and products.",
  identifier_columns = c("customer_id", "product_id", "time"),
  key_outcome_specs = list(
    list(
      OutcomeName = "Revenue",
      ValueExpression = quote(price * quantity),
      AggregationMethods = list(
        list(AggregatedName = "TotalRevenue", AggregationFunction = "sum", GroupingVariables = character(0)),
        list(AggregatedName = "TotalRevenueByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id"),
        list(AggregatedName = "TotalRevenueByProduct",  AggregationFunction = "sum", GroupingVariables = "product_id")
      )
    ),
    list(
      OutcomeName = "TransactionCount",
      ValueExpression = 1,
      AggregationMethods = list(
        list(AggregatedName = "TotalTransactions", AggregationFunction = "sum", GroupingVariables = character(0)),
        list(AggregatedName = "TransactionsByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id")
      )
    ),
    list(
      OutcomeName = "UnitsSold",
      ValueExpression = quote(quantity),
      AggregationMethods = list(
        list(AggregatedName = "TotalUnitsSold", AggregationFunction = "sum", GroupingVariables = character(0)),
        list(AggregatedName = "UnitsSoldByProduct", AggregationFunction = "sum", GroupingVariables = "product_id")
      )
    )
  )
)


customers_entry <- define_table_meta(
  table_name = 'customers',
  source_identifier = 'customers.csv',
  identifier_columns = 'customer_id',
  key_outcome_specs = list(
    list(
      OutcomeName = "CustomerDemographics",
      ValueExpression = quote(age),
      AggregationMethods = list(
        list(
          AggregatedName = "AverageAge",
          AggregationFunction = "mean",
          GroupingVariables = character(0)
          )
          )
    )
  )  
)


product_entry <- define_table_meta(
  table_name = 'products',
  source_identifier = 'products.csv',
  identifier_columns = 'product_id',
  key_outcome_specs = list(
  list(
    OutcomeName = "Averageprice",
    ValueExpression = quote(original_price),
    AggregationMethods = list(
  list(
    AggregatedName = "AverageoriginalPrice",
    AggregationFunction = "mean",
    GroupingVariables = character(0)
  ),
  list(
    AggregatedName = "AverageoriginalPriceByCategory",
    AggregationFunction = "mean",
    GroupingVariables = "category"
  )
)
)
)
)



views_entry <- define_table_meta(
  table_name = 'product_views',
  source_identifier = 'views.csv',
  description = 'It contains the views of the products',
  identifier_columns = c('customer_id','product_id','time'),
  key_outcome_specs = list(
    list(
      OutcomeName = "ViewEvent",
      ValueExpression = 1,
      AggregationMethods = list(
        list(
          AggregatedName = "TotalProductViews",
          AggregationFunction = "sum",
          GroupingVariables = character(0)
        ),
        list(
          AggregatedName = "productViewsByProduct",
          AggregationFunction = "sum", 
          GroupingVariables = "product_id"
        ),
        list(
          AggregatedName = "productViewsByCustomer",
          AggregationFunction = "sum", 
          GroupingVariables = "customer_id"
        )
      )
    ),
    list(
      OutcomeName = "uniqueProductViewed", 
      ValueExpression = quote(product_id),
      AggregationMethods = list(
        list(
          AggregatedName = "totaluniqueProductViewed",
          AggregationFunction = "n_distinct",
          GroupingVariables = character(0)
        ),
        list(
          AggregatedName = "uniqueProductViewedByCustomer",
          AggregationFunction = "n_distinct",
          GroupingVariables = "customer_id"
        )
      )
    )
  )
)







if (!exists("table_of_tables")) {
  table_of_tables <- list()
}

table_of_tables[["transactions"]] <- transactions_entry
table_of_tables[["customers"]] <- customers_entry
table_of_tables[['products']] <- product_entry
table_of_tables[['views']] <- views_entry
# Verification remains the same
print("Transactions metadata defined:")
str(table_of_tables[["transactions"]])
str(table_of_tables[["customers"]])
str(table_of_tables[["products"]])
str(table_of_tables[["views"]])
print("Full Table of Tables content:")
str(table_of_tables)
print(table_of_tables[['transactions']])
print(table_of_tables[['customers']])
print(table_of_tables[['products']])
print(table_of_tables[['views']])
