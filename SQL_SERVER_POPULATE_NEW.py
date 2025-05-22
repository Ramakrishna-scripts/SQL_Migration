import pyodbc
from faker import Faker
import random

# Mapping of SQL Server data types
SQLSERVER_DATA_TYPES = {
    "int": "INT",
    "tinyint": "TINYINT",
    "smallint": "SMALLINT",
    "bigint": "BIGINT",
    "decimal": "DECIMAL(10, 2)",
    "float": "FLOAT",
    "double": "FLOAT",  # SQL Server does not have a separate double type; it uses float
    "varchar": "VARCHAR(255)",
    "text": "VARCHAR(MAX)",  # SQL Server does not have a TEXT type; use VARCHAR(MAX) instead
    "char": "CHAR(10)",
    "date": "DATE",
    "datetime": "DATETIME2",  # This will be skipped in insertions
    "json": "VARCHAR(MAX)"  # SQL Server does not have a native JSON type; use VARCHAR(MAX) instead
}

def create_connection():
    try:
        connection = pyodbc.connect(
            driver='{ODBC Driver 17 for SQL Server}',
            server='localhost',  # Change this to your SQL Server address
            database='my_db',
            uid='sa',  # Replace with your SQL Server username
            pwd='Admin@1234'  # Replace with your SQL Server password
        )
        print("Connected to SQL Server")
        return connection
    except pyodbc.Error as e:
        print("Error while connecting to SQL Server", e)
        return None

def check_table_exists(connection, table_name):
    cursor = connection.cursor()
    query = """
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?
    """
    cursor.execute(query, table_name)
    result = cursor.fetchone()
    return result[0] > 0

def create_tables(connection):
    cursor = connection.cursor()
    for i in range(1, 11):
        table_name = f"table_{i}"
        if not check_table_exists(connection, table_name):
            columns = ", ".join(
                f"col_{key} {value}" for key, value in SQLSERVER_DATA_TYPES.items()
            )
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    id INT PRIMARY KEY IDENTITY(1,1),
                    {columns},
                    created_at DATETIME2 DEFAULT SYSDATETIME(),
                    updated_at DATETIME2 DEFAULT SYSDATETIME()
                )
            """)
            print(f"Table {table_name} created")
        else:
            print(f"Table {table_name} already exists")
    connection.commit()

def insert_random_data(connection):
    cursor = connection.cursor()
    fake = Faker()
    for i in range(1, 11):
        table_name = f"table_{i}"
        for _ in range(100):
            values = []
            columns_to_insert = []
            for key in SQLSERVER_DATA_TYPES.keys():
                if key == "datetime":
                    continue  # Skip datetime columns since they will be auto-filled
                if key == "int":
                    values.append(random.randint(1, 100))
                elif key == "tinyint":
                    values.append(random.choice([0, 1]))
                elif key == "smallint":
                    values.append(random.randint(-100, 100))
                elif key == "bigint":
                    values.append(random.randint(1, 1000000))
                elif key == "decimal":
                    values.append(round(random.uniform(1.0, 100.0), 2))
                elif key == "float":
                    values.append(random.uniform(1.0, 100.0))
                elif key == "double":
                    values.append(random.uniform(1.0, 1000.0))
                elif key == "varchar":
                    values.append(fake.text(max_nb_chars=20))
                elif key == "text":
                    values.append(fake.text(max_nb_chars=50))
                elif key == "char":
                    values.append(fake.text(max_nb_chars=10))
                elif key == "date":
                    values.append(fake.date())
                elif key == "json":
                    values.append('{"key": "value"}')
                columns_to_insert.append(f"col_{key}")

            # Do not include created_at and updated_at in the insert statement
            placeholders = ", ".join(["?"] * len(values))
            columns = ", ".join(columns_to_insert)
            cursor.execute(
                f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})",
                tuple(values)
            )
        print(f"Inserted 100 rows into {table_name}")
    connection.commit()

def main():
    connection = create_connection()
    if connection:
        create_tables(connection)
        insert_random_data(connection)
        connection.close()
        print("Data insertion completed.")

if __name__ == "__main__":
    main()
