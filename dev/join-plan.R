library(data.table)



# Easy examples
customers_entry1 <- table_info(
  table_name = "customers",
  source_identifier = "customers.csv",
  identifier_columns = "customer_id",
  key_outcome_specs = list(
    list(OutcomeName = "CustomerCount", ValueExpression = 1, AggregationMethods = list(
      list(AggregatedName = "CountByRegion", AggregationFunction = "sum", GroupingVariables = "region")
    ))
  )
)

products_entry1 <- table_info(
  table_name = "products",
  source_identifier = "products.csv",
  identifier_columns = "product_id",
  key_outcome_specs = list(
    list(OutcomeName = "ProductCount", ValueExpression = 1, AggregationMethods = list(
      list(AggregatedName = "CountByCategory", AggregationFunction = "sum", GroupingVariables = "category")
    ))
  )
)

transactions_entry1 <- table_info(
  table_name = "transactions",
  source_identifier = "transactions.csv",
  identifier_columns = c("customer_id", "product_id", "time"),
  key_outcome_specs = list(
    list(
      OutcomeName = "Revenue", ValueExpression = quote(price * quantity),
      AggregationMethods = list(
        list(AggregatedName = "RevenueByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id"),
        list(AggregatedName = "RevenueByProduct", AggregationFunction = "sum", GroupingVariables = "product_id"),
        list(AggregatedName = "DailyRevenueByProduct", AggregationFunction = "sum",
             GroupingVariables = c("product_id", "time"))
      )
    )
  )
)

views_entry1 <- table_info(
  table_name = "views",
  source_identifier = "views.csv",
  identifier_columns = c("customer_id", "product_id", "time"),
  key_outcome_specs = list(
    list(
      OutcomeName = "ViewCount", ValueExpression = quote(1),
      AggregationMethods = list(
        list(AggregatedName = "ViewsByCustomer", AggregationFunction = "count", GroupingVariables = "customer_id"),
        list(AggregatedName = "ViewsByProduct", AggregationFunction = "count", GroupingVariables = "product_id"),
        list(AggregatedName = "DailyViewsByCustomerProduct", AggregationFunction = "sum",
             GroupingVariables = c("customer_id", "product_id", "time"))
      )
    )
  )
)
meta1 <- create_metadata_registry()
meta1 <- add_table(meta1, customers_entry1)
meta1 <- add_table(meta1, products_entry1)
meta1 <- add_table(meta1, views_entry1)
meta1 <- add_table(meta1, transactions_entry1)
join1 <- map_join_paths(meta1)
# 1.
TtoP <- create_join_plan(
  "products", selections = list(
    products = "product_id",
    transactions = "RevenueByProduct"
  ),
  metadata_dt = meta1,
  join_map = join1
)


print(TtoP)


# 2.
TtoC <- create_join_plan(
  "customers", selections = list(
    customers= "customer_id",
    transactions = "RevenueByCustomer"
  ),
  metadata_dt = meta1,
  join_map = join1
)


print(TtoC)

# 3.
VtoP <- create_join_plan(
  "products", selections = list(
    products= "product_id",
    views = "ViewsByProduct"
  ),
  metadata_dt = meta1,
  join_map = join1
)


print(VtoP)


# 4.
VtoC <- create_join_plan(
  "customers", selections = list(
    customers= "customer_id",
    views = "ViewsByCustomer"
  ),
  metadata_dt = meta1,
  join_map = join1
)


print(VtoC)





# Challenging Examples

customers_entry2 <- table_info(
  table_name = "customers",
  source_identifier = "customers.csv",
  identifier_columns = c("customer_id", "date"),
  key_outcome_specs = list(
    list(OutcomeName = "CustomerCount", ValueExpression = 1, AggregationMethods = list(
      list(AggregatedName = "CountByRegion", AggregationFunction = "sum", GroupingVariables = "region")
    ))
  )
)

products_entry2 <- table_info(
  table_name = "products",
  source_identifier = "products.csv",
  identifier_columns = c("product_id", "date"),
  key_outcome_specs = list(
    list(OutcomeName = "ProductCount", ValueExpression = 1, AggregationMethods = list(
      list(AggregatedName = "CountByCategory", AggregationFunction = "sum", GroupingVariables = "category")
    ))
  )
)

transactions_entry2 <- table_info(
  table_name = "transactions",
  source_identifier = "transactions.csv",
  identifier_columns = c("customer_id", "product_id", "time"),
  key_outcome_specs = list(
    list(
      OutcomeName = "Revenue", ValueExpression = quote(price * quantity),
      AggregationMethods = list(
        list(AggregatedName = "RevenueByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id"),
        list(AggregatedName = "RevenueByProduct", AggregationFunction = "sum", GroupingVariables = "product_id"),
        list(AggregatedName = "DailyRevenueByProduct", AggregationFunction = "sum",
             GroupingVariables = c("product_id", "date")),
        list(AggregatedName = "DailyRevenueByCustomer", AggregationFunction = "sum",
             GroupingVariables = c("customer_id", "date"))
      )
    )
  )
)

views_entry2 <- table_info(
  table_name = "views",
  source_identifier = "views.csv",
  identifier_columns = c("customer_id", "product_id", "time"),
  key_outcome_specs = list(
    list(
      OutcomeName = "ViewCount", ValueExpression = quote(1),
      AggregationMethods = list(
        list(AggregatedName = "ViewsByCustomer", AggregationFunction = "count", GroupingVariables = "customer_id"),
        list(AggregatedName = "ViewsByProduct", AggregationFunction = "count", GroupingVariables = "product_id"),
        list(AggregatedName = "DailyViewsByProduct", AggregationFunction = "sum",
             GroupingVariables = c("product_id", "date")),
        list(AggregatedName = "DailyViewsByCustomer", AggregationFunction = "sum",
             GroupingVariables = c("customer_id", "date"))
      )
    )
  )
)
meta2 <- create_metadata_registry()
meta2 <- add_table(meta2, customers_entry2)
meta2 <- add_table(meta2, products_entry2)
meta2 <- add_table(meta2, views_entry2)
meta2 <- add_table(meta2, transactions_entry2)
join2 <- map_join_paths(meta2)

# 1.
PtoT <- create_join_plan(
  "products", selections = list(
    products = c("product_id", "date"),
    transactions = "DailyRevenueByProduct"
  ),
  metadata_dt = meta2,
  join_map = join2
)

print(PtoT)


# 2.
PtoV <- create_join_plan(
  "products", selections = list(
    products = c("product_id", "date"),
    views = "DailyViewsByProduct"
  ),
  metadata_dt = meta2
)

print(PtoV)


# 3.
CtoT <- create_join_plan(
  "customers", selections = list(
    customers = c("customer_id", "date"),
    transactions = "DailyRevenueByCustomer"
  ),
  metadata_dt = meta2
)

print(CtoT)


# 4.

CtoV <- create_join_plan(
  "customers", selections = list(
    customers = c("customer_id", "date"),
    views = "DailyViewsByCustomer"
  ),
  metadata_dt = meta2
)

print(CtoV)




# Difficult examples
customers_entry3 <- table_info(
  table_name = "customers",
  source_identifier = "customers.csv",
  identifier_columns = "customer_id",
  key_outcome_specs = list(
    list(OutcomeName = "CustomerCount", ValueExpression = 1, AggregationMethods = list(
      list(AggregatedName = "CountByRegion", AggregationFunction = "sum", GroupingVariables = "region")
    ))
  )
)

products_entry3 <- table_info(
  table_name = "products",
  source_identifier = "products.csv",
  identifier_columns = "product_id",
  key_outcome_specs = list(
    list(OutcomeName = "ProductCount", ValueExpression = 1, AggregationMethods = list(
      list(AggregatedName = "CountByCategory", AggregationFunction = "sum", GroupingVariables = "category")
    ))
  )
)

transactions_entry3 <- table_info(
  table_name = "transactions",
  source_identifier = "transactions.csv",
  identifier_columns = c("customer_id", "product_id", "time"),
  key_outcome_specs = list(
    list(
      OutcomeName = "Revenue", ValueExpression = quote(price * quantity),
      AggregationMethods = list(
        list(AggregatedName = "RevenueByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id"),
        list(AggregatedName = "RevenueByProduct", AggregationFunction = "sum", GroupingVariables = "product_id"),
        list(AggregatedName = "DailyRevenueByProduct", AggregationFunction = "sum",
             GroupingVariables = c("product_id", "time"))
      )
    )
  )
)

views_entry3 <- table_info(
  table_name = "views",
  source_identifier = "views.csv",
  identifier_columns = c("customer_id", "product_id", "time"),
  key_outcome_specs = list(
    list(
      OutcomeName = "ViewCount", ValueExpression = quote(1),
      AggregationMethods = list(
        list(AggregatedName = "ViewsByCustomer", AggregationFunction = "count", GroupingVariables = "customer_id"),
        list(AggregatedName = "ViewsByProduct", AggregationFunction = "count", GroupingVariables = "product_id"),
        # MULTI variable grouping -> joins to 'customer_product_interactions'
        list(AggregatedName = "DailyViewsByCustomerProduct", AggregationFunction = "sum",
             GroupingVariables = c("customer_id", "product_id", "time"))
      )
    )
  )
)
meta <- create_metadata_registry()
meta <- add_table(meta, customers_entry3)
meta <- add_table(meta, products_entry3)
meta <- add_table(meta, views_entry3)
meta <- add_table(meta, transactions_entry3)
join <- map_join_paths(meta)

# 1.

cust1 <- create_join_plan("customers", selections = list(
          customers = "customer_id",
          transactions = "RevenueByCustomer",
          views = "ViewsByCustomer"
        ),
        metadata_dt = meta,
        join_map = join
)

print(cust1)

# 2.
prod1 <- create_join_plan("products", selections = list(
  products = "product_id",
  transactions = "RevenueByProduct",
  views = "ViewsByProduct"
  ),
  metadata_dt = meta,
  )

  print(prod1)

# 3.

cus2 <- create_join_plan(
  "customers", selections = list(
    customers = c("customer_id", "age", "region"),
    transactions = "RevenueByCustomer",
    views = "ViewsByCustomer"
),
metadata_dt = meta,
join_map = join
)

print(cus2)

# 4.

prod2 <- create_join_plan(
  "products", selections = list(
    products = c("product_id", "category"),
    transactions = "RevenueByProduct",
    views = "ViewsByProduct"
),
metadata_dt = meta,
join_map = join
)

print(prod2)