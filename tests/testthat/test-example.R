library(testthat)
library(DBmaps)

test_that("package loads successfully", {
  expect_true(requireNamespace("DBmaps", quietly = TRUE))
})