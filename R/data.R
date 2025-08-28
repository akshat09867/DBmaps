#' Sample Customer Data
#'
#' A sample dataset containing demographic information for customers included with
#' the DBmaps package.
#'
#' @format A data.table with 5 variables:
#' \describe{
#'   \item{customer_id}{A unique identifier for each customer.}
#'   \item{age}{The age of the customer in years.}
#'   \item{gender}{The gender of the customer.}
#'   \item{income}{The income level of the customer.}
#'   \item{region}{The geographical region where the customer resides.}
#' }
#' @source Generated for package examples.
"customers"


#' Sample Product Data
#'
#' A sample dataset containing product information included with the DBmaps package.
#'
#' @format A data.table with 3 variables:
#' \describe{
#'   \item{product_id}{A unique identifier for each product.}
#'   \item{category}{The category to which the product belongs.}
#'   \item{original_price}{The original price of the product.}
#' }
#' @source Generated for package examples.
"products"


#' Sample Transaction Data
#'
#' A sample dataset of transaction events, linking customers and products. This
#' is a typical "fact" table in a relational schema.
#'
#' @format A data.table with 5 variables:
#' \describe{
#'   \item{customer_id}{Identifier for the customer making the transaction.}
#'   \item{product_id}{Identifier for the product being purchased.}
#'   \item{time}{The timestamp of the transaction (POSIXct format).}
#'   \item{quantity}{The number of units of the product purchased.}
#'   \item{price}{The price per unit at the time of transaction.}
#' }
#' @source Generated for package examples.
"transactions"


#' Sample Product View Data
#'
#' A sample dataset of product view events, linking customers and products. This
#' is a smaller, sampled version of a potentially very large event log.
#'
#' @format A data.table with 3 variables:
#' \describe{
#'   \item{customer_id}{Identifier for the customer viewing the product.}
#'   \item{product_id}{Identifier for the product being viewed.}
#'   \item{time}{The timestamp of the view event (POSIXct format).}
#' }
#' @source Generated for package examples.
"views"