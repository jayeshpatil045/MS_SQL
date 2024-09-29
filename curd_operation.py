'''
@Author: Jayesh Patil
@Date: 2024-09-27
@Last Modified by: Jayesh Patil
@Title: Perform CURD operation 

'''

import pyodbc

def create_connection(server_name, database_name=None):
    """
    Description:
        Creates a connection to a SQL Server instance.

    Parameters:
        server_name (str): The name of the SQL Server instance.
        database_name (str, optional): The name of the database to connect to. Defaults to None.

    Returns:
        connection (pyodbc.Connection): The connection object if successful, otherwise None.
    """
    connection_string = f'Driver={{ODBC Driver 17 for SQL Server}};Server={server_name};'
    if database_name:
        connection_string += f'Database={database_name};'
    connection_string += 'Trusted_Connection=yes;'

    try:
        connection = pyodbc.connect(connection_string)
        print("Connection successful!")
        return connection
    except pyodbc.Error as e:
        print("Error in connection: ", e)
        return None

def create_database(connection, database_name):
    """
    Description:
        Creates a new database in the connected SQL Server.

    Parameters:
        connection (pyodbc.Connection): The connection object.
        database_name (str): The name of the database to be created.

    Returns:
        None
    """
    try:
        connection.autocommit = True  
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE {database_name}")
        cursor.close()
        print(f"Database '{database_name}' created successfully.")
    except pyodbc.Error as e:
        print("Error creating database: ", e)
    finally:
        connection.autocommit = False


def view_databases(connection):
    """
    Description:
        Retrieves and displays the list of all databases available on the connected SQL Server.

    Parameters:
        connection (pyodbc.Connection): The connection object.

    Returns:
        None
    """
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT name FROM sys.databases")
        databases = cursor.fetchall()
        print("Available Databases:")
        for db in databases:
            print(db[0])
    except pyodbc.Error as e:
        print(f"Error retrieving databases: {e}")
    finally:
        cursor.close()

def create_table(connection, table_name):
    """
    Description:
        Creates a new table with specified columns in the selected database.

    Parameters:
        connection (pyodbc.Connection): The connection object.
        table_name (str): The name of the table to be created.

    Returns:
        None
    """
    cursor = connection.cursor()
    try:
        columns = input("Enter columns and their types (e.g., first_name NVARCHAR(50), last_name NVARCHAR(50)): ")
        query = f"CREATE TABLE {table_name} (id INT PRIMARY KEY IDENTITY(1,1), {columns})"
        cursor.execute(query)
        connection.commit()
        print(f"Table '{table_name}' created successfully.")
    except pyodbc.Error as e:
        print(f"Error creating table: {e}")
    finally:
        cursor.close()

def get_table_structure(connection, table_name):
    """
    Description:
        Retrieves the structure (columns and data types) of a specific table.

    Parameters:
        connection (pyodbc.Connection): The connection object.
        table_name (str): The name of the table.

    Returns:
        columns (list): A list of tuples containing column names and their data types.
    """
    cursor = connection.cursor()
    query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
    cursor.execute(query)
    columns = cursor.fetchall()
    cursor.close()
    return columns

def insert_values_into_table(connection, table_name, columns):
    """
    Description:
        Inserts values into a specified table based on the column structure.

    Parameters:
        connection (pyodbc.Connection): The connection object.
        table_name (str): The name of the table where data is to be inserted.
        columns (list): The column structure of the table.

    Returns:
        None
    """
    cursor = connection.cursor()

    values = []
    for column in columns[1:]: 
        column_name, data_type = column
        value = input(f"Enter value for {column_name} ({data_type}): ")
        if data_type == 'int':
            value = int(value)
        values.append(value)

    placeholders = ', '.join(['?'] * len(values))
    query = f"INSERT INTO {table_name} ({', '.join([col[0] for col in columns[1:]])}) VALUES ({placeholders})"
    cursor.execute(query, values)
    connection.commit()
    print("Data inserted successfully.")
    cursor.close()

def read_table_data(connection, table_name):
    """
    Description:
        Retrieves and displays all data from a specific table.

    Parameters:
        connection (pyodbc.Connection): The connection object.
        table_name (str): The name of the table from which to read data.

    Returns:
        None
    """
    cursor = connection.cursor()
    query = f"SELECT * FROM {table_name}"
    cursor.execute(query)
    results = cursor.fetchall()
    for row in results:
        print(row)
    cursor.close()

def update_table(connection, table_name, columns):
    """
    Description:
        Updates data in a specific table based on matching conditions.

    Parameters:
        connection (pyodbc.Connection): The connection object.
        table_name (str): The name of the table to update.
        columns (list): The column structure of the table.

    Returns:
        None
    """
    cursor = connection.cursor()
    
    update_conditions = []
    for column in columns[1:]:  
        column_name, _ = column
        condition_value = input(f"Enter value to match for {column_name} to update row (leave blank to skip): ")
        if condition_value:
            update_conditions.append(f"{column_name} = '{condition_value}'")

    set_conditions = []
    for column in columns[1:]:  
        column_name, data_type = column
        new_value = input(f"Enter new value for {column_name} (leave blank to skip): ")
        if new_value:
            if data_type == 'int':
                new_value = int(new_value)
            set_conditions.append(f"{column_name} = '{new_value}'")

    if update_conditions and set_conditions:
        where_clause = " AND ".join(update_conditions)
        set_clause = ", ".join(set_conditions)
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        cursor.execute(query)
        connection.commit()
        print("Row updated successfully.")
    else:
        print("No matching conditions or values provided.")

    cursor.close()

def delete_row_from_table(connection, table_name, columns):
    """
    Description:
        Deletes rows from a specific table based on matching conditions.

    Parameters:
        connection (pyodbc.Connection): The connection object.
        table_name (str): The name of the table from which to delete data.
        columns (list): The column structure of the table.

    Returns:
        None
    """
    cursor = connection.cursor()

    delete_conditions = []
    for column in columns[1:]:  
        column_name, _ = column
        condition_value = input(f"Enter value to match for {column_name} to delete row (leave blank to skip): ")
        if condition_value:
            delete_conditions.append(f"{column_name} = '{condition_value}'")

    if delete_conditions:
        where_clause = " AND ".join(delete_conditions)
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        cursor.execute(query)
        connection.commit()
        print("Row deleted successfully.")
    else:
        print("No matching conditions provided.")

    cursor.close()

def main():
    """
    Description:
        Main function that acts as the entry point of the program. It handles the user interface 
        for creating databases, viewing databases, and working with tables (creating, inserting, 
        updating, deleting, and viewing data).

    Parameters:
        None

    Returns:
        None
    """
    server_name = input("Enter your server name (e.g., MSI\\SQLEXPRESS01): ")
    connection = create_connection(server_name)

    if connection:
        while True:
            print("\nMain Menu:")
            print("1. Create Database")
            print("2. View Databases")
            print("3. Select Database")
            print("4. Exit")

            choice = input("Select an option (1-4): ")

            if choice == '1':
                db_name = input("Enter database name to create: ")
                create_database(connection, db_name)

            elif choice == '2':
                view_databases(connection)

            elif choice == '3':
                db_name = input("Enter the database name to select: ")
                db_connection = create_connection(server_name, db_name)
                if db_connection:
                    while True:
                        print("\nTable Menu:")
                        print("1. Create Table")
                        print("2. Select Table")
                        print("3. Back to Main Menu")

                        table_choice = input("Select an option (1-3): ")

                        if table_choice == '1':
                            table_name = input("Enter table name to create: ")
                            create_table(db_connection, table_name)

                        elif table_choice == '2':
                            table_name = input("Enter the table name to select: ")
                            columns = get_table_structure(db_connection, table_name)
                            if columns:
                                while True:
                                    print(f"\nSelected Table: {table_name}")
                                    print("1. Add Data to Table")
                                    print("2. Update Data in Table")
                                    print("3. Delete Data from Table")
                                    print("4. View Table Data")
                                    print("5. Back to Table Menu")

                                    sub_table_choice = input("Select an option (1-5): ")

                                    if sub_table_choice == '1':
                                        insert_values_into_table(db_connection, table_name, columns)

                                    elif sub_table_choice == '2':
                                        update_table(db_connection, table_name, columns)

                                    elif sub_table_choice == '3':
                                        delete_row_from_table(db_connection, table_name, columns)

                                    elif sub_table_choice == '4':
                                        read_table_data(db_connection, table_name)

                                    elif sub_table_choice == '5':
                                        print("Returning to table menu.")
                                        break

                                    else:
                                        print("Invalid choice. Please select a valid option.")

                            else:
                                print(f"Table '{table_name}' does not exist or has no columns.")

                        elif table_choice == '3':
                            print("Returning to main menu.")
                            break

                        else:
                            print("Invalid choice. Please select a valid option.")

                else:
                    print("Failed to connect to the specified database.")

            elif choice == '4':
                print("Exiting the program.")
                break

            else:
                print("Invalid choice. Please select a valid option.")

        connection.close()

if __name__ == "__main__":
    main()
