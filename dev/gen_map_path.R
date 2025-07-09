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
        # SINGLE variable grouping -> joins to 'customers'
        list(AggregatedName = "RevenueByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id"),
        # SINGLE variable grouping -> joins to 'products'
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
        # SINGLE variable grouping -> joins to 'customers'
        list(AggregatedName = "ViewsByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id"),
        # SINGLE variable grouping -> joins to 'products'
        list(AggregatedName = "ViewsByProduct", AggregationFunction = "sum", GroupingVariables = "product_id"),
        # MULTI variable grouping -> joins to 'customer_product_interactions'
        list(AggregatedName = "DailyViewsByCustomerProduct", AggregationFunction = "sum",
             GroupingVariables = c("customer_id", "product_id", "time"))
      )
    )
  )
)

# --- 4. Combine Metadata and Discover Paths ---

master_metadata_dt <- rbindlist(list(
  customers_entry,
  products_entry,
  transactions_entry,
  views_entry
))

join_map <- map_join_paths(master_metadata_dt)

print(join_map)




# --- Sample Data ---

customers <- data.table(
  customer_id = c("c1","c2","c3"), 
  region      = c("NA", "EU", "NA")
)

products <- data.table(
  sku      = c("p1","p2","p3"), 
  category = c("A", "B", "A")
)

transactions <- data.table(
  customer_id  = c("c1","c2","c1","c3"),
  product_code = c("p1","p3","p2","p1"),
  revenue      = c(10, 50, 20, 15)
)

data_list_example <- list(
  customers    = customers,
  products     = products,
  transactions = transactions
)

# --- Metadata Definition ---

# 1) customers table
customers_meta <- table_info(
  "customers", "cust.csv", 
  identifier_columns    = "customer_id",
  key_outcome_specs   = list(
    list(
      OutcomeName        = "x",
      ValueExpression    = 1,
      AggregationMethods = list(
        list(
          AggregatedName      = "y",
          AggregationFunction = "z",
          GroupingVariables   = "region"
        )
      )
    )
  )
)

# 2) products table
products_meta <- table_info(
  "products", "prod.csv",
  identifier_columns    = "sku",
  key_outcome_specs   = list(
    list(
      OutcomeName        = "x",
      ValueExpression    = 1,
      AggregationMethods = list(
        list(
          AggregatedName      = "y",
          AggregationFunction = "z",
          GroupingVariables   = "category"
        )
      )
    )
  )
)

# 3) transactions table
transactions_meta <- table_info(
  "transactions", "trans.csv", 
  identifier_columns    = "transaction_id",
  key_outcome_specs   = list(
    list(
      OutcomeName        = "x",
      ValueExpression    = 1,
      AggregationMethods = list(
        list(
          AggregatedName      = "y",
          AggregationFunction = "z",
          GroupingVariables   = c("customer_id", "product_code")
        )
      )
    )
  )
)



master_meta_example <- rbindlist(list(
  customers_meta, products_meta, transactions_meta
))

cat(" --- Run the enhanced function ---\n")
all_discovered_paths <- map_join_paths(master_meta_example, data_list_example)
print(all_discovered_paths)
