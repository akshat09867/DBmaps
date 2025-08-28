# DBmaps 0.1.0

*   Initial release of `DBmaps`.
*   Added `table_info()` to define table metadata and aggregations.
*   Added `map_join_paths()` to discover join paths from metadata and, optionally, from data (inferred joins).
*   Added `create_join_plan()` to translate a user's request into a step-by-step, executable plan.
*   Added `execute_join_plan()` to run the generated plan and produce a final `data.table`.
*   Added `plot_join_plan()` to visualize the execution plan as a flowchart.
*   Added four sample datasets (`customers`, `products`, `transactions`, `views`) for examples and testing.
*   Added a main vignette (`DBmaps-introduction`) demonstrating the full package workflow.
*   Submitted to CRAN.