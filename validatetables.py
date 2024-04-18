import json
import mariadb
import psycopg2

def save_connection_data(filename, data):
    with open(filename, 'w') as f:
        json.dump(data, f)

def get_database_connection_info(filename):
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {}
    return data.get('connector_type'), data.get('host'), data.get('db'), data.get('user'), data.get('password')

def get_connector(connector_type):
    if connector_type == "mariadb":
        return mariadb
    elif connector_type == "postgres":
        return psycopg2

def validate_databases(connector_type1, host1, database1, user1, password1, connector_type2, host2, database2, user2, password2):
    connector1 = get_connector(connector_type1)
    connector2 = get_connector(connector_type2)

    conn1 = None
    conn2 = None

    try:
        print("Connecting to databases...")
        conn1 = connector1.connect(host=host1, user=user1, password=password1, database=database1)
        conn2 = connector2.connect(host=host2, user=user2, password=password2, database=database2)
        print("Databases connected successfully.")

        cursor1 = conn1.cursor()
        cursor2 = conn2.cursor()

        table1_input = input("Enter the first table name: ")
        table2_input = input("Enter the second table name: ")

        # Validate if the tables exist in both databases
        print("Validating tables...")
        cursor1.execute("SELECT 1 FROM information_schema.tables WHERE table_name = %s", (table1_input,))
        cursor2.execute("SELECT 1 FROM information_schema.tables WHERE table_name = %s", (table2_input,))

        if not cursor1.fetchone() or not cursor2.fetchone():
            print(f"Validation failed: Table '{table1_input}' or '{table2_input}' doesn't exist")
            return False

        print("Tables validated successfully.")

        # Fetch column names and data types for table 1
        cursor1.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", (table1_input,))
        columns1 = cursor1.fetchall()

        # Fetch column names and data types for table 2
        cursor2.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", (table2_input,))
        columns2 = cursor2.fetchall()

        # Compare column names and data types
        if columns1 != columns2:
            print("Validation failed: Column names or data types do not match.")
            print("Columns in database 1:")
            for column in columns1:
                print(column)
            print("Columns in database 2:")
            for column in columns2:
                print(column)
            return False

        # Fetch number of records for table 1
        cursor1.execute(f"SELECT COUNT(*) FROM {table1_input}")
        count1 = cursor1.fetchone()[0]

        # Fetch number of records for table 2
        cursor2.execute(f"SELECT COUNT(*) FROM {table2_input}")
        count2 = cursor2.fetchone()[0]

        # Compare number of records
        if count1 != count2:
            print("Validation failed: Number of records do not match.")
            print(f"Number of records in {table1_input}: {count1}")
            print(f"Number of records in {table2_input}: {count2}")
            return False

        print("Validation successful: Databases are identical.")

    except Exception as e:
        print(f"Validation failed: {e}")

    finally:
        if conn1:
            conn1.close()
        if conn2:
            conn2.close()

def main():
    connector_type1, host1, db1, user1, password1 = get_database_connection_info("source_connection.json")
    connector_type2, host2, db2, user2, password2 = get_database_connection_info("target_connection.json")

    print("0. Enter Database Connection Data")
    print("2. Validate")
    option = input("Choose an option: ")

    if option == "0":
        connector_type1 = input("Enter source connector type (mariadb/postgres): ")
        host1 = input("Enter source host: ")
        db1 = input("Enter source database: ")
        user1 = input("Enter source username: ")
        password1 = input("Enter source password: ")
        connector_type2 = input("Enter target connector type (mariadb/postgres): ")
        host2 = input("Enter target host: ")
        db2 = input("Enter target database: ")
        user2 = input("Enter target username: ")
        password2 = input("Enter target password: ")
        save_connection_data("source_connection.json", {"connector_type": connector_type1, "host": host1, "db": db1, "user": user1, "password": password1})
        save_connection_data("target_connection.json", {"connector_type": connector_type2, "host": host2, "db": db2, "user": user2, "password": password2})

    elif option == "2":
        validate_databases(connector_type1, host1, db1, user1, password1, connector_type2, host2, db2, user2, password2)

    else:
        print("Invalid option")

if __name__ == "__main__":
    main()
