# Query Executor

## Overview
The Query Executor is a Python application that allows users to interact with tabular data using SQL-like commands. It provides a simple command-line interface as well as a graphical user interface (GUI) built with Tkinter. The application supports various operations such as fetching data, defining tables, inserting values, removing rows, aggregating data, grouping by columns, and joining tables.

## Features
- **SQL-Like Commands:** Use commands similar to SQL for querying and manipulating data.
- **Table Operations:** Define tables, insert values, remove rows, and display available tables.
- **Aggregation:** Aggregate data using functions such as COUNT, SUM, MIN, MAX, and AVG.
- **Grouping:** Group data by a specified column and perform aggregation.
- **Joins:** Perform JOIN operations on two tables based on a common column.

## How to Use
1. **Install Dependencies:**
   - Ensure you have the required Python libraries installed. You can install them using the following:
     ```bash
     pip install pandas tabulate tkinter memory-profiler
     ```

2. **Run the Application:**
   - Execute the main Python script (`query_executor.py`) to run the command-line interface.
   - For the GUI interface, run the script and interact with the provided graphical interface.

3. **Commands:**
   - Enter SQL-like commands to interact with tables and perform operations.
   - Examples:
     - `FETCH ALL FROM table_name`
     - `FETCH column_name FROM table_name`
     - `DEFINE TABLE table_name NO_VALUES column1 column2 ...`
     - `ADD INTO table_name VALUES value1 value2 value3 ...`
     - `REMOVE FROM table_name WHERE condition`
     - `MODIFY TABLE table_name SET column_name = value WHERE condition`
     - `REMOVE TABLE table_name`
     - `DISPLAY TABLES`
     - `AGGREGATE table_name aggregation_function column_name`
     - `GROUP BY table_name BY aggregation_function column_name`
     - `JOIN table1 table2 ON common_column`

4. **GUI Usage:**
   - The GUI provides a text entry for queries, a button to execute queries, and a scrollable text area for results.
   - Enter queries in the text entry, click "Execute Query," and view results in the text area.

5. **Exit:**
   - To exit the application, enter 'exit' when prompted for a query in the command-line interface.
   - For the GUI, click the "Exit" button.

## Examples
### Fetch All Data
```sql
FETCH ALL FROM air_quality_data
```

### Fetch Specific Columns with Condition
```sql
FETCH column1, column2 FROM air_quality_data FOR column3 = value
```

### Define a New Table
```sql
DEFINE TABLE new_table NO_VALUES column1 column2 column3
```

### Insert Values into a Table
```sql
ADD INTO existing_table VALUES value1 value2 value3
```

### Remove Rows from a Table
```sql
REMOVE FROM existing_table WHERE column = value
```

### Modify Table Data
```sql
MODIFY TABLE existing_table SET column1 = new_value WHERE column2 > value
```

### Remove a Table
```sql
REMOVE TABLE table_name
```

### Display Available Tables
```sql
DISPLAY TABLES
```

### Aggregate Data
```sql
AGGREGATE air_quality_data COUNT column_name
```

### Group Data
```sql
GROUP BY air_quality_data BY COUNT column_name
```

### Join Tables
```sql
JOIN table1 table2 ON common_column
```

## Additional Notes
- The application uses CSV files to store table data, and tables are loaded into memory with chunking for efficient handling of large datasets.
- Memory profiling is incorporated for tracking memory usage during the execution of queries.
- The GUI provides a user-friendly interface for executing queries and viewing results. It is a basic implementation and can be enhanced for a more improved performance.

Feel free to explore and experiment with different queries using the Query Executor!
