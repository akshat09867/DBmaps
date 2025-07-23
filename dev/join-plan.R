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
        # SINGLE variable grouping -> joins to 'customers'
        list(AggregatedName = "RevenueByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id"),
        # SINGLE variable grouping -> joins to 'products'
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
        # SINGLE variable grouping -> joins to 'customers'
        list(AggregatedName = "ViewsByCustomer", AggregationFunction = ".N", GroupingVariables = "customer_id"),
        # SINGLE variable grouping -> joins to 'products'
        list(AggregatedName = "ViewsByProduct", AggregationFunction = ".N", GroupingVariables = "product_id"),
        # MULTI variable grouping -> joins to 'customer_product_interactions'
        list(AggregatedName = "DailyViewsByCustomerProduct", AggregationFunction = "sum",
             GroupingVariables = c("customer_id", "product_id", "time"))
      )
    )
  )
)
meta <- rbindlist(list(customers_entry1, products_entry1, transactions_entry1, views_entry1))

join1 <- map_join_paths(meta)
# 1.
TtoP <- create_join_plan(
  "products", selections = list(
    products = "product_id",
    transactions = "RevenueByProduct"
  ),
  metadata_dt = meta,
  join_map = join1
)


print(TtoP)


# 2.
TtoC <- create_join_plan(
  "customers", selections = list(
    customers= "customer_id",
    transactions = "RevenueByCustomer"
  ),
  metadata_dt = meta,
  join_map = join1
)


print(TtoC)

# 3.
VtoP <- create_join_plan(
  "products", selections = list(
    products= "product_id",
    views = "ViewsByProduct"
  ),
  metadata_dt = meta,
  join_map = join1
)


print(VtoP)


# 4.
VtoC <- create_join_plan(
  "customers", selections = list(
    customers= "customer_id",
    views = "ViewsByCustomer"
  ),
  metadata_dt = meta,
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
        # SINGLE variable grouping -> joins to 'customers'
        list(AggregatedName = "RevenueByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id"),
        # SINGLE variable grouping -> joins to 'products'
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
        # SINGLE variable grouping -> joins to 'customers'
        list(AggregatedName = "ViewsByCustomer", AggregationFunction = ".N", GroupingVariables = "customer_id"),
        # SINGLE variable grouping -> joins to 'products'
        list(AggregatedName = "ViewsByProduct", AggregationFunction = ".N", GroupingVariables = "product_id"),
        # MULTI variable grouping -> joins to 'customer_product_interactions'
        list(AggregatedName = "DailyViewsByProduct", AggregationFunction = "sum",
             GroupingVariables = c("product_id", "date")),
        list(AggregatedName = "DailyViewsByCustomer", AggregationFunction = "sum",
             GroupingVariables = c("customer_id", "date"))
      )
    )
  )
)
meta2 <- rbindlist(list(customers_entry2, products_entry2, transactions_entry2, views_entry2))
j2 <- map_join_paths(meta2)

# 1.
PtoT <- create_join_plan(
  "products", selections = list(
    products = c("product_id", "date"),
    transactions = "DailyRevenueByProduct"
  ),
  metadata_dt = meta2,
  join_map = j2
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
        # SINGLE variable grouping -> joins to 'customers'
        list(AggregatedName = "RevenueByCustomer", AggregationFunction = "sum", GroupingVariables = "customer_id"),
        # SINGLE variable grouping -> joins to 'products'
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
        # SINGLE variable grouping -> joins to 'customers'
        list(AggregatedName = "ViewsByCustomer", AggregationFunction = ".N", GroupingVariables = "customer_id"),
        # SINGLE variable grouping -> joins to 'products'
        list(AggregatedName = "ViewsByProduct", AggregationFunction = ".N", GroupingVariables = "product_id"),
        # MULTI variable grouping -> joins to 'customer_product_interactions'
        list(AggregatedName = "DailyViewsByCustomerProduct", AggregationFunction = "sum",
             GroupingVariables = c("customer_id", "product_id", "time"))
      )
    )
  )
)
master_metadata_dt <- rbindlist(list(
  customers_entry3,
  products_entry3,
  transactions_entry3,
  views_entry3
))

join <- map_join_paths(master_metadata_dt)

# 1.

cust1 <- create_join_plan("customers", selections = list(
          customers = "customer_id",
          transactions = "RevenueByCustomer",
          views = "ViewsByCustomer"
        ),
        metadata_dt = master_metadata_dt,
        join_map = join
)

print(cust1)

# 2.
prod1 <- create_join_plan("products", selections = list(
  products = "product_id",
  transactions = "RevenueByProduct",
  views = "ViewsByProduct"
  ),
  metadata_dt = master_metadata_dt,
  )

  print(prod1)

# 3.

cus2 <- create_join_plan(
  "customers", selections = list(
    customers = c("customer_id", "age", "region"),
    transactions = "RevenueByCustomer",
    views = "ViewsByCustomer"
),
metadata_dt = master_metadata_dt,
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
metadata_dt = master_metadata_dt,
join_map = join
)

print(prod2)