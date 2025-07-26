library(testthat)
library(data.table)


# Metadata Generation
customers_meta <- table_info("customers", "c.csv", "customer_id", list(list(OutcomeName="x",ValueExpression=1,AggregationMethods=list(list(AggregatedName="y",AggregationFunction="z",GroupingVariables="region")))))
products_meta <- table_info("products", "p.csv", "product_id", list(list(OutcomeName="x",ValueExpression=1,AggregationMethods=list(list(AggregatedName="y",AggregationFunction="z",GroupingVariables="category")))))
multi_dim_meta <- table_info("multi_dim", "m.csv", c("dim1", "dim2"), list(list(OutcomeName="x",ValueExpression=1,AggregationMethods=list(list(AggregatedName="y",AggregationFunction="z",GroupingVariables="dim1")))))
transactions_meta <- table_info("transactions", "t.csv", "trans_id", list(
  list(OutcomeName="rev",ValueExpression=1,AggregationMethods=list(
    list(AggregatedName="a",AggregationFunction="sum",GroupingVariables="customer_id"),
    list(AggregatedName="b",AggregationFunction="sum",GroupingVariables="product_id"),
    list(AggregatedName="c",AggregationFunction="sum",GroupingVariables=c("dim1", "dim2"))
  ))
))

# Mismatched names metadata for inferred tests
inventory_meta <- table_info("inventory", "i.csv", "sku", list(list(OutcomeName="x",ValueExpression=1,AggregationMethods=list(list(AggregatedName="y",AggregationFunction="z",GroupingVariables="category")))))
orders_meta <- table_info("orders", "o.csv", "order_id", list(list(OutcomeName="x",ValueExpression=1,AggregationMethods=list(list(AggregatedName="y",AggregationFunction="z",GroupingVariables="customer_reference")))))


# Data Generation
customers_data <- data.table(customer_id = c("c1", "c2", "c3"))
products_data <- data.table(product_id = c("p1", "p2"))
inventory_data <- data.table(sku = c("s1", "s2", "s3"))
multi_dim_data <- data.table(dim1 = "a", dim2 = "b")
transactions_data <- data.table(trans_id=1:4, customer_id=c("c1","c2","c1","c3"), product_id=c("p1","p1","p2","p2"), dim1="a", dim2="b")
orders_data <- data.table(order_id=1:2, customer_reference=c("s1","s2"))


# --- Test Cases ---

test_that("finds basic single-key metadata joins", {
  meta <- rbindlist(list(customers_meta, products_meta, transactions_meta))
  paths <- map_join_paths(meta)
  
  expect_equal(nrow(paths), 2)
  expect_true(all(paths$type == "METADATA"))
  expect_true(any(paths$table_from == "transactions" & paths$table_to == "customers"))
  expect_true(any(paths$table_from == "transactions" & paths$table_to == "products"))
})

test_that("finds multi-key metadata joins", {
  meta <- rbindlist(list(multi_dim_meta, transactions_meta))
  paths <- map_join_paths(meta)
  
  expect_equal(nrow(paths), 1)
  expect_equal(paths$table_from, "transactions")
  expect_equal(paths$table_to, "multi_dim")
  expect_equal(paths$key_from[[1]], c("dim1", "dim2"))
  expect_equal(paths$key_to[[1]], c("dim1", "dim2"))
})

test_that("handles no possible joins gracefully", {
  meta <- rbindlist(list(customers_meta, products_meta))
  expect_warning(paths <- map_join_paths(meta), "No potential join paths were found.")
  expect_equal(nrow(paths), 0)
})

test_that("finds inferred joins when data is provided", {
  meta <- rbindlist(list(inventory_meta, orders_meta))
  data <- list(inventory = inventory_data, orders = orders_data)
  
  paths <- map_join_paths(meta, data)
  
  expect_equal(nrow(paths), 1)
  expect_equal(paths$table_from, "orders")
  expect_equal(paths$table_to, "inventory")
  expect_equal(paths$key_from[[1]], "customer_reference")
  expect_equal(paths$key_to[[1]], "sku")
})

test_that("combines and de-duplicates metadata and inferred joins correctly", {
  
  # Data setup where transactions references inventory by a different name
  transactions_with_sku_ref <- data.table(
    trans_id=1:2,
    customer_id=c("c1","c2"),
    product_sku=c("s1","s3")
  )
  
  # Metadata setup
  trans_sku_meta <- table_info("transactions", "t.csv", "trans_id", list(
    list(OutcomeName="rev",ValueExpression=1,AggregationMethods=list(
      list(AggregatedName="a",AggregationFunction="sum",GroupingVariables="customer_id"),
      list(AggregatedName="b",AggregationFunction="sum",GroupingVariables="product_sku")
    ))
  ))
  meta <- rbindlist(list(customers_meta, inventory_meta, trans_sku_meta))
  data <- list(
    customers = customers_data, 
    inventory = inventory_data, 
    transactions = transactions_with_sku_ref
  )
  
  paths <- map_join_paths(meta, data)
  
  expect_equal(nrow(paths), 2)
  
  meta_path <- paths[table_to == "customers",]
  expect_equal(meta_path$key_from[[1]], "customer_id")
  
  inferred_path <- paths[table_to == "inventory",]
  expect_equal(inferred_path$key_from[[1]], "product_sku")
  expect_equal(inferred_path$key_to[[1]], "sku")
})

test_that("function throws errors for invalid input", {
  expect_error(map_join_paths("not a data.table"), "'metadata_dt' must be a data.table.")
  
  meta <- rbindlist(list(customers_meta))
  expect_error(map_join_paths(meta, data_list = "not a list"), "'data_list' must be a named list of data.tables.")
  
  unnamed_list <- list(customers_data)
  expect_error(map_join_paths(meta, data_list = unnamed_list), "'data_list' must be a named list of data.tables.")
})