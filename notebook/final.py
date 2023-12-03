import os
import pandas as pd
from tabulate import tabulate
import warnings
warnings.filterwarnings('ignore')
import tkinter as tk
from tkinter import scrolledtext
from memory_profiler import profile


air_quality_data = '../data/air_quality_data.csv'
tables = {}

def fetch_all_data(table):
    return table

# def fetch_column_data(table, column_names, query_parts, chunk_size=10000):
#     conditions_index = query_parts.index('FOR') if 'FOR' in query_parts else None

#     if conditions_index is not None:
#         if len(query_parts) >= conditions_index + 4:
#             column_name = query_parts[conditions_index + 1]
#             condition_sign = query_parts[conditions_index + 2]
#             condition_value = query_parts[conditions_index + 3]

#             valid_conditions = ['<', '>', '=']
#             if condition_sign not in valid_conditions:
#                 return "Error: Invalid filter condition. Only <, >, and = are supported."

#             try:
#                 condition_value = float(condition_value)
#             except ValueError:
#                 return "Error: Condition value must be a numeric value."

#             try:
#                 if condition_sign == '=':
#                     result = table[table[column_name] == condition_value][column_names]
#                 elif condition_sign == '<':
#                     result = table[table[column_name] < condition_value][column_names]
#                 elif condition_sign == '>':
#                     result = table[table[column_name] > condition_value][column_names]
#                 else:
#                     return "Error: Invalid condition sign. Supported signs are '=', '<', and '>'."

#                 return result

#             except KeyError as e:
#                 return f"Error: Column '{column_name}' not found in the table."

#         else:
#             return "Error: Incomplete FOR condition."

#     else:
#         try:
#             if isinstance(table, pd.DataFrame):
#                 return table[column_names]
#             else:
#                 # Handle fetching columns from chunked data
#                 chunks = read_csv_in_chunks(table, chunk_size=chunk_size)
#                 result_chunks = []

#                 for chunk in chunks:
#                     result_chunk = chunk[column_names]
#                     result_chunks.append(result_chunk)

#                 result = pd.concat(result_chunks, ignore_index=True)
#                 return result

#         except KeyError as e:
#             return f"Error: Column '{e.args[0]}' not found in the table."


def fetch_column_data(table, column_names, query_parts, chunk_size=10000):
    conditions_index = query_parts.index('FOR') if 'FOR' in query_parts else None

    if conditions_index is not None:
        if len(query_parts) >= conditions_index + 4:
            column_name = query_parts[conditions_index + 1]
            condition_sign = query_parts[conditions_index + 2]
            condition_value = query_parts[conditions_index + 3]

            valid_conditions = ['<', '>', '=']
            if condition_sign not in valid_conditions:
                return "Error: Invalid filter condition. Only <, >, and = are supported."

            try:
                condition_value = float(condition_value)
            except ValueError:
                return "Error: Condition value must be a numeric value."

            try:
                if condition_sign == '=':
                    result = table[table[column_name] == condition_value][column_names]
                elif condition_sign == '<':
                    result = table[table[column_name] < condition_value][column_names]
                elif condition_sign == '>':
                    result = table[table[column_name] > condition_value][column_names]
                else:
                    return "Error: Invalid condition sign. Supported signs are '=', '<', and '>'."

                return result

            except KeyError as e:
                return f"Error: Column '{column_name}' not found in the table."

        else:
            return "Error: Incomplete FOR condition."

    else:
        try:
            if isinstance(table, pd.DataFrame):
                return table[column_names]
            else:
                # Handle fetching columns from chunked data
                chunks = read_csv_in_chunks(table, chunk_size=chunk_size)
                result_chunks = []

                for chunk in chunks:
                    result_chunk = chunk[column_names]
                    result_chunks.append(result_chunk)

                result = pd.concat(result_chunks, ignore_index=True)
                return result

        except KeyError as e:
            return f"Error: Column '{e.args[0]}' not found in the table."



def read_csv_in_chunks(file_path, chunk_size):
    chunks = []
    reader = pd.read_csv(file_path, chunksize=chunk_size)
    for chunk in reader:
        chunks.append(chunk)
    return pd.concat(chunks, ignore_index=True)


def insert_into_table(table_name, values):
    if table_name not in tables:
        return f"Error: Table '{table_name}' not found."

    table_df = tables[table_name]
    # Check if the number of values matches the number of columns
    if len(values) != len(table_df.columns):
        return "Error: Number of values doesn't match the number of columns."

    # Create a dictionary with column names as keys and corresponding values
    row_data = dict(zip(table_df.columns, values))

    # Convert the row data to a DataFrame and append it to the existing table
    new_row = pd.DataFrame([row_data], columns=table_df.columns)

    # Append the new row to the existing table in chunks
    table_df = pd.concat([table_df, new_row], ignore_index=True)

    # Update the table in the tables dictionary
    tables[table_name] = table_df

    # Update the CSV file in chunks
    chunk_size = 10000
    num_chunks = len(table_df) // chunk_size + 1
    for i in range(num_chunks):
        chunk = table_df.iloc[i * chunk_size:(i + 1) * chunk_size]
        chunk.to_csv(f'../data/{table_name}_chunk_{i}.csv', index=False)

    return f"Values inserted into table '{table_name}'."



def remove_from_table(table_name, condition, air_quality_data):
    if table_name not in tables:
        return f"Error: Table '{table_name}' not found."

    table_df = tables[table_name]

    try:
        # Split the condition into column, operator, and value
        column, operator, value = condition.split()

        # Convert the value to the appropriate type
        try:
            value = float(value)
        except ValueError:
            return f"Error: Condition value must be a numeric value."

        # Initialize an empty list to store chunks after removal
        updated_chunks = []

        # Iterate over chunks and apply the condition
        for i in range(len(table_df) // 10000 + 1):
            chunk = table_df.iloc[i * 10000:(i + 1) * 10000]
            condition_mask = (chunk[column] == value) if operator == '=' else (
                    chunk[column] < value if operator == '<' else chunk[column] > value
            )
            chunk = chunk[~condition_mask]
            updated_chunks.append(chunk)

        # Concatenate updated chunks
        table_df = pd.concat(updated_chunks, ignore_index=True)

        # Update the table in the tables dictionary
        tables[table_name] = table_df

        # Update the CSV file in chunks
        chunk_size = 10000
        num_chunks = len(table_df) // chunk_size + 1
        for i in range(num_chunks):
            chunk = table_df.iloc[i * chunk_size:(i + 1) * chunk_size]
            chunk.to_csv(f'../data/{table_name}_chunk_{i}.csv', index=False)

        return f"Rows removed from table '{table_name}' based on condition: '{condition}'."
    except Exception as e:
        return f"Error: Invalid condition '{condition}'. {str(e)}"

def update_table(table_name, set_column, set_value, where_column, where_operator, where_value):
    if table_name not in tables:
        return f"Error: Table '{table_name}' not found."

    table_df = tables[table_name]

    try:
        # Convert the set value to the appropriate type
        try:
            set_value = float(set_value)
        except ValueError:
            return f"Error: SET value must be a numeric value."

        # Initialize an empty list to store chunks after update
        updated_chunks = []

        # Iterate over chunks and apply the update
        for i in range(len(table_df) // 10000 + 1):
            chunk = table_df.iloc[i * 10000:(i + 1) * 10000]
            condition_mask = (chunk[where_column] == float(where_value)) if where_operator == '=' else (
                    chunk[where_column] < float(where_value) if where_operator == '<' else chunk[where_column] > float(where_value)
            )
            chunk.loc[condition_mask, set_column] = set_value
            updated_chunks.append(chunk)

        # Concatenate updated chunks
        table_df = pd.concat(updated_chunks, ignore_index=True)

        # Update the table in the tables dictionary
        tables[table_name] = table_df

        # Update the CSV file in chunks
        chunk_size = 10000
        num_chunks = len(table_df) // chunk_size + 1
        for i in range(num_chunks):
            chunk = table_df.iloc[i * chunk_size:(i + 1) * chunk_size]
            chunk.to_csv(f'../data/{table_name}_chunk_{i}.csv', index=False)

        return f"Table '{table_name}' updated."

    except Exception as e:
        return f"Error: Unable to update table '{table_name}'. {str(e)}"
    

def aggregate_table(table_name, aggregation_function, column):
    if table_name not in tables:
        return f"Error: Table '{table_name}' not found."

    table_data = tables[table_name]

    try:
        column_data = table_data[column]

        if aggregation_function == 'COUNT':
            result = len(column_data)
        elif aggregation_function == 'SUM':
            result = sum(column_data)
        elif aggregation_function == 'MIN':
            result = min(column_data)
        elif aggregation_function == 'MAX':
            result = max(column_data)
        elif aggregation_function == 'AVG':
            result = sum(column_data) / len(column_data)
        else:
            return f"Error: Invalid aggregation function '{aggregation_function}'. Supported functions are COUNT, SUM, MIN, MAX, and AVG."

        return aggregation_function, result

    except KeyError as e:
        return f"Error: Column '{column}' not found in the table."


def remove_table(table_name):
    if table_name in tables:
        # Remove the table from the tables dictionary
        del tables[table_name]

        # Remove the CSV file
        csv_path = f'../data/{table_name}.csv'
        if os.path.exists(csv_path):
            os.remove(csv_path)

        return f"Table '{table_name}' removed."

    else:
        return f"Error: Table '{table_name}' not found."


def join_tables(table1_name, table2_name, join_column):
    if table1_name not in tables or table2_name not in tables:
        return f"Error: One or both tables not found."

    table1 = tables[table1_name]
    table2 = tables[table2_name]

    try:
        if join_column not in table1.columns or join_column not in table2.columns:
            return f"Error: Join column '{join_column}' not found in one or both tables."

        # Perform the JOIN operation
        result_table = pd.merge(table1, table2, on=join_column)

        # Generate a unique name for the result table
        result_table_name = f"{table1_name}_{table2_name}_JOIN"

        # Save the result table to the tables dictionary
        tables[result_table_name] = result_table

        # Save the result table to a CSV file
        result_table.to_csv(f'../data/{result_table_name}.csv', index=False)

        return f"Tables '{table1_name}' and '{table2_name}' joined on column '{join_column}'. Result table: '{result_table_name}'."

    except Exception as e:
        return f"Error: Unable to perform JOIN operation. {str(e)}"


# def join_tables(table1_name, table2_name, join_column, chunk_size=10000):
#     if table1_name not in tables or table2_name not in tables:
#         return f"Error: One or both tables not found."

#     table1 = tables[table1_name]
#     table2 = tables[table2_name]

#     try:
#         if join_column not in table1.columns or join_column not in table2.columns:
#             return f"Error: Join column '{join_column}' not found in one or both tables."

#         result_table_chunks = []

#         # Iterate over chunks of both tables
#         for i in range(len(table1) // chunk_size + 1):
#             chunk1 = table1.iloc[i * chunk_size:(i + 1) * chunk_size]

#             for j in range(len(table2) // chunk_size + 1):
#                 chunk2 = table2.iloc[j * chunk_size:(j + 1) * chunk_size]

#                 # Perform the JOIN operation on the chunks
#                 result_chunk = pd.merge(chunk1, chunk2, on=join_column)
#                 result_table_chunks.append(result_chunk)

#         # Concatenate the result chunks
#         result_table = pd.concat(result_table_chunks, ignore_index=True)

#         # Generate a unique name for the result table
#         result_table_name = f"{table1_name}_{table2_name}_JOIN"

#         # Save the result table to the tables dictionary
#         tables[result_table_name] = result_table

#         # Save the result table to a CSV file in chunks
#         num_chunks = len(result_table) // chunk_size + 1
#         for i in range(num_chunks):
#             chunk = result_table.iloc[i * chunk_size:(i + 1) * chunk_size]
#             chunk.to_csv(f'../data/{result_table_name}_chunk_{i}.csv', index=False)

#         return f"Tables '{table1_name}' and '{table2_name}' joined on column '{join_column}'. Result table: '{result_table_name}'."

#     except Exception as e:
#         return f"Error: Unable to perform JOIN operation. {str(e)}"




def group_by_table(table_name, aggregation_function, column):
    if table_name not in tables:
        return f"Error: Table '{table_name}' not found."

    table_data = tables[table_name]

    try:
        unique_values = table_data[column].unique()

        grouped_data = pd.DataFrame(columns=[column, aggregation_function])

        for value in unique_values:
            if aggregation_function == 'COUNT':
                result = len(table_data[table_data[column] == value])
            elif aggregation_function == 'SUM':
                result = sum(table_data[table_data[column] == value][column])
            else:
                return f"Error: Invalid aggregation function '{aggregation_function}'. Supported functions are COUNT and SUM."

            grouped_data = grouped_data._append({column: value, aggregation_function: result}, ignore_index=True)

        return grouped_data

    except KeyError as e:
        return f"Error: Column '{column}' not found in the table."

@profile
def execute_query(query, demographics_data, reading_data, air_quality_data):
    query_parts = query.split()

    if len(query_parts) >= 5 and query_parts[0] == 'JOIN':
        table1_name = query_parts[1]
        table2_name = query_parts[2]
        on_keyword = query_parts[3]
        join_column = query_parts[4]

        if on_keyword.upper() != 'ON':
            return "Error: Expected 'ON' keyword in JOIN operation."

        return join_tables(table1_name, table2_name, join_column)

    if len(query_parts) >= 6 and query_parts[0] == 'GROUP' and query_parts[1] == 'BY' and query_parts[3] == 'BY':
        table_name = query_parts[2]
        aggregation_function = query_parts[4]
        column = query_parts[5]

        return group_by_table(table_name, aggregation_function, column)

    elif len(query_parts) >= 4 and query_parts[0] == 'AGGREGATE':
        table_name = query_parts[1]
        aggregation_function = query_parts[2]
        column = query_parts[3]

        return aggregate_table(table_name, aggregation_function, column)

    elif len(query_parts) >= 7 and query_parts[0] == 'MODIFY' and query_parts[1] == 'TABLE':
        table_name = query_parts[2]
        set_column = query_parts[4]
        set_value = query_parts[6]
        where_column = query_parts[8]
        where_operator = query_parts[9]
        where_value = query_parts[10]

        return update_table(table_name, set_column, set_value, where_column, where_operator, where_value)

    elif len(query_parts) >= 5 and query_parts[0] == 'REMOVE' and query_parts[1] == 'FROM':
        table_name = query_parts[2]
        if table_name in tables:
            table = tables[table_name]
        elif table_name == 'demographics_data':
            table = demographics_data
        elif table_name == 'reading_data':
            table = reading_data
        else:
            return f"Error: Table '{table_name}' not found."

        if query_parts[3] == 'WHERE':
            condition = ' '.join(query_parts[4:])
            result = remove_from_table(table_name, condition, air_quality_data)
            return result
        else:
            return "Error: Invalid REMOVE command. Expected WHERE clause."

    elif len(query_parts) >= 4 and query_parts[0] == 'FETCH' and query_parts[2] == 'FROM':
        table_name = query_parts[3]
        if table_name in tables:
            table = tables[table_name]
        elif table_name == 'demographics_data':
            table = demographics_data
        elif table_name == 'reading_data':
            table = reading_data
        else:
            return f"Error: Table '{table_name}' not found."

        if query_parts[1] == 'ALL':
            result = fetch_all_data(table)
        else:
            column_names = query_parts[1].split(',')
            result = fetch_column_data(table, column_names, query_parts)

            # Check if sorting is specified
            if 'INC' in query_parts or 'DEC' in query_parts:
                sort_order = 'INC' if 'INC' in query_parts else 'DEC'
                result = sorted(result.values.tolist(), key=lambda x: x[0], reverse=(sort_order == 'DEC'))
                result = pd.DataFrame(result, columns=column_names)

        return result

    elif query_parts[0] == 'DEFINE' and query_parts[1] == 'TABLE':
        table_name = query_parts[2]
        if table_name in tables:
            return f"Error: Table '{table_name}' already exists."

        # Check if col_names are provided in the query
        if len(query_parts) >= 4:
            columns = query_parts[3:]
            result = define_table(table_name, columns, air_quality_data)
            return result

        else:
            return "Error: No column names provided for the new table."

    elif query_parts[0] == 'ADD' and query_parts[1] == 'INTO':
        table_name = query_parts[2]
        values = query_parts[4:]  # Starting from index 4 to skip 'VALUES'
        return insert_into_table(table_name, values)

    elif query_parts[0] == 'REMOVE' and query_parts[1] == 'TABLE':
        table_name = query_parts[2]
        return remove_table(table_name)

    elif query_parts[0] == 'DISPLAY' and query_parts[1] == 'TABLES':
        return display_tables()

    else:
        return "Invalid query"

def details():
    print("\n--- Query Details ---")
    print("1. FETCH ALL FROM table_name: Fetches all data from the specified table.")
    print("2. FETCH column_name FROM table_name: Fetches the specified column from the table.")
    print("3. FETCH column_name FROM table_name FOR column_name = value: Fetches the specified column with a condition.")
    print("   Supported conditions: <, >, =")
    print("4. DEFINE TABLE table_name NO_VALUES column1 column2 ...: Defines a new table with the specified columns and saves it as a CSV file with no values.")
    print("   Use 'ALL_VALUES' to include all columns from the air quality data.")
    print("5. ADD INTO table_name VALUES value1 value2 value3 ...: Inserts values into the specified table.")
    print("6. REMOVE FROM table_name WHERE condition: Removes rows from the specified table based on the condition.")
    print("7. MODIFY TABLE table_name SET column_name = value WHERE condition: Updates values in the specified table based on the condition.")
    print("   Supported conditions: <, >, =")
    print("8. REMOVE TABLE table_name: Removes the specified table.")
    print("9. DISPLAY TABLES: Shows a list of available tables.")
    print("10. AGGREGATE table_name aggregation_function column_name: Performs aggregation on the specified column.")
    print("   Supported aggregation functions: COUNT, SUM, MIN, MAX, AVG.")
    print("11. GROUP BY table_name BY aggregation_function column_name: Performs grouping on the specified column.")
    print("   Supported aggregation functions: COUNT, SUM.")
    print("12. JOIN table1 table2 ON common_column: Performs joining on the specified tables.")



def display_tables():
    table_info = []
    for table_name, table in tables.items():
        table_info.append({'Table Name': table_name, 'Columns': ', '.join(table.columns)})

    if table_info:
        table_df = pd.DataFrame(table_info)
        return tabulate(table_df, headers='keys', tablefmt='psql')
    else:
        return "No tables defined."



# def define_table(table_name, columns, air_quality_data):
#     if table_name in tables:
#         return f"Error: Table '{table_name}' already exists."

#     allowed_column_names = air_quality_data.columns.tolist()

#     if 'NO_VALUES' in columns:
#         # Remove 'NO_VALUES' from the list of columns
#         columns.remove('NO_VALUES')

#         if columns:  # If specific columns are provided, create a table with those columns
#             # Check if entered column names are in the allowed list
#             invalid_columns = [col for col in columns if col not in allowed_column_names]
#             if invalid_columns:
#                 return f"Error: Invalid column names {', '.join(invalid_columns)}. Allowed column names are {', '.join(allowed_column_names)}."

#             # Create a new table with the specified columns
#             new_table = pd.DataFrame(columns=columns)
#         else:  # If no specific columns are provided, create a table with all columns
#             new_table = pd.DataFrame(columns=allowed_column_names)

#     elif 'ALL_VALUES' in columns:
#         # Include all columns from the air quality data
#         new_table = air_quality_data.copy()

#     else:
#         # Check if entered column names are in the allowed list
#         invalid_columns = [col for col in columns if col not in allowed_column_names]
#         if invalid_columns:
#             return f"Error: Invalid column names {', '.join(invalid_columns)}. Allowed column names are {', '.join(allowed_column_names)}."

#         # Create a new table with the specified columns
#         new_table = air_quality_data[columns].copy()

#     # Save the new table to the tables dictionary
#     tables[table_name] = new_table

#     # Save the new table to a CSV file
#     new_table.to_csv(f'../data/{table_name}.csv', index=False)

#     return f"Table '{table_name}' defined with columns: {', '.join(columns)}."


def define_table(table_name, columns, air_quality_data, chunk_size=10000):
    if table_name in tables:
        return f"Error: Table '{table_name}' already exists."

    allowed_column_names = air_quality_data.columns.tolist()

    if 'NO_VALUES' in columns:
        # Remove 'NO_VALUES' from the list of columns
        columns.remove('NO_VALUES')

        if columns:  # If specific columns are provided, create a table with those columns
            # Check if entered column names are in the allowed list
            invalid_columns = [col for col in columns if col not in allowed_column_names]
            if invalid_columns:
                return f"Error: Invalid column names {', '.join(invalid_columns)}. Allowed column names are {', '.join(allowed_column_names)}."

            # Create an empty DataFrame with the specified columns
            new_table = pd.DataFrame(columns=columns)
        else:  # If no specific columns are provided, create a table with all columns
            new_table = pd.DataFrame(columns=allowed_column_names)

    elif 'ALL_VALUES' in columns:
        # Include all columns from the air quality data
        new_table = air_quality_data.copy()

    else:
        # Check if entered column names are in the allowed list
        invalid_columns = [col for col in columns if col not in allowed_column_names]
        if invalid_columns:
            return f"Error: Invalid column names {', '.join(invalid_columns)}. Allowed column names are {', '.join(allowed_column_names)}."

        # Create an empty DataFrame with the specified columns
        new_table = air_quality_data[columns].copy()

    # Save the new table to the tables dictionary
    tables[table_name] = new_table

    # Save the new table to a CSV file in chunks
    num_chunks = len(new_table) // chunk_size + 1
    for i in range(num_chunks):
        chunk = new_table.iloc[i * chunk_size:(i + 1) * chunk_size]
        chunk.to_csv(f'../data/{table_name}_chunk_{i}.csv', index=False)

    return f"Table '{table_name}' defined with columns: {', '.join(columns)}."



def load_existing_tables():
    # Specify the directory where CSV files are stored
    csv_directory = '../data/'

    # Loop through all CSV files in the directory
    for csv_file in os.listdir(csv_directory):
        if csv_file.endswith(".csv"):
            # Extract table name from the file name
            table_name = os.path.splitext(csv_file)[0]

            # Load the CSV file into a DataFrame with chunking
            table_data = read_csv_in_chunks(os.path.join(csv_directory, csv_file), chunk_size=10000)

            # Save the DataFrame in the tables dictionary
            tables[table_name] = table_data




class QueryExecutorApp:
    def __init__(self, master):
        self.master = master
        master.title("Query Executor")

        # Set the initial size of the window (width x height)
        master.geometry("1000x800")

        self.query_entry = tk.Entry(master, width=50)
        self.query_entry.pack(pady=10)

        # Increase the height of the text area
        self.result_text = scrolledtext.ScrolledText(master, width=120, height=30)
        self.result_text.pack(pady=10)

        self.execute_button = tk.Button(master, text="Execute Query", command=self.execute_query)
        self.execute_button.pack(pady=5)

        self.details_button = tk.Button(master, text="Query Details", command=self.show_details)
        self.details_button.pack(pady=5)

        self.exit_button = tk.Button(master, text="Exit", command=master.destroy)
        self.exit_button.pack(pady=5)


    def execute_query(self):
        user_query = self.query_entry.get()
        result = execute_query(user_query, tables.get('demographics_data'), tables.get('reading_data'),
                               tables.get('air_quality_data'))

        self.result_text.delete(1.0, tk.END)  # Clear previous results
        if isinstance(result, pd.DataFrame):
            result_text = tabulate(result, headers='keys', tablefmt='psql', showindex=False)
        else:
            result_text = str(result)

        self.result_text.insert(tk.END, result_text)

    def show_details(self):
        details_text = """
        1. FETCH ALL FROM table_name: Fetches all data from the specified table.
        2. FETCH column_name FROM table_name: Fetches the specified column from the table.
        3. FETCH column_name FROM table_name FOR column_name = value: Fetches the specified column with a condition.
           Supported conditions: <, >, =
        4. DEFINE TABLE table_name NO_VALUES column1 column2 ...: Defines a new table with the specified columns and saves it as a CSV file with no values.
           Use 'ALL_VALUES' to include all columns from the air quality data.
        5. ADD INTO table_name VALUES value1 value2 value3 ...: Inserts values into the specified table.
        6. REMOVE FROM table_name WHERE condition: Removes rows from the specified table based on the condition.
        7. MODIFY TABLE table_name SET column_name = value WHERE condition: Updates values in the specified table based on the condition.
           Supported conditions: <, >, =
        8. REMOVE TABLE table_name: Removes the specified table.
        9. DISPLAY TABLES: Shows a list of available tables.
        10. AGGREGATE table_name aggregation_function column_name: Performs aggregation on the specified column.
           Supported aggregation functions: COUNT, SUM, MIN, MAX, AVG.
        11. GROUP BY table_name BY aggregation_function column_name: Performs grouping on the specified column.
           Supported aggregation functions: COUNT, SUM.
        12. JOIN table1 table2 ON common_column: Performs joining on the specified tables.
        """
        self.result_text.delete(1.0, tk.END)  # Clear previous results
        self.result_text.insert(tk.END, details_text)


def main():
    # Load existing tables from CSV files
    load_existing_tables()

    root = tk.Tk()
    app = QueryExecutorApp(root)
    root.mainloop()


    while True:
        user_query = input("Enter your query (or 'exit' to exit) or ('details' for details): ")

        if user_query.lower() == 'exit':
            break

        elif user_query.lower() == 'details':
            details()
            break

        # Pass air_quality_data as an argument when executing the query
        result = execute_query(user_query, tables.get('demographics_data'), tables.get('reading_data'), tables.get('air_quality_data'))
        print("Result:")
        if isinstance(result, pd.DataFrame):
            print(tabulate(result, headers='keys', tablefmt='psql', showindex=False))
        else:
            print(result)

if __name__ == "__main__":
    main()
