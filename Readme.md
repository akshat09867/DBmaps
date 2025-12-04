# DBmaps: Automated Relational Database Join Path Discovery and Mapping

DBmaps (AutoJoins) is an R package designed to automate the tedious work of exploring, organizing, and combining data stored in relational databases. It simplifies database analysis by automatically discovering join paths, mapping table relationships, and performing complex multi-table merges.

## 🧐 The Problem
Analyzing data from relational databases often requires joining and merging various tables to unify scattered information.

- **Complex Structures:** You have to know exactly how tables relate and which unique keys link them.  
- **Manual Effort:** In complex systems, data is spread across many tables, forcing you to link them step-by-step manually.  
- **Missing Tools:** While R has `merge()` and `dplyr`, there is no easy way to automatically figure out how tables are related or create a map of those relationships.

## 🚀 How DBmaps Helps You

DBmaps solves these complexities by providing tools to quickly understand your database structure and use it effectively.

### Key Benefits

- **Automated Discovery:** Automatically identify which tables can be joined and suggest the most likely linking variables using fuzzy matching and value overlap.  
- **Visual Mapping:** Generate clear, intuitive visualizations of your database schema to grasp potential relationships at a glance.  
- **Pathfinding:** Automatically find valid join paths between distant tables using graph search algorithms (BFS).  
- **Simplified Merging:** Perform sequential merges of multiple tables without writing complex, nested code.
