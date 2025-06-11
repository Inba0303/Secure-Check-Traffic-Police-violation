import psycopg2
import pandas as pd # Assuming 'df' is a pandas DataFrame

# --- Database Connection ---
try:
    connection = psycopg2.connect(
        host='localhost',
        port=5432,
        user='postgres',
        password='root123',
        database='lone_db'
    )
    cursor = connection.cursor()
    print("Connected to PostgreSQL successfully!")

    # --- Create Database (if it doesn't exist) and connect to it ---
    # In psycopg2, you connect to an existing database. You typically create
    # a new database outside of your application code or connect to 'postgres'
    # and then issue a CREATE DATABASE command.
    # For this example, we assume 'lone_db' already exists or will be created
    # by other means. If 'lone_db' doesn't exist, this connection will fail.

    # --- Table Creation ---

    # Issue 1: "create table if not exists traffic_stops" is incomplete.
    # It should define columns.
    # Issue 2: "use traffic_stops;" is not a PostgreSQL command in this context.
    # You specify the database in the connection string.
    
    # Corrected traffic_stops table (assuming this is where the main vehicle data goes)
    # The original "check_post_logs" seems to be the one you're inserting into.
    # Let's clarify the table names. Based on your insert statement,
    # it looks like you want to insert into `check_post_logs`.
    # I've named the main vehicle data table as `check_post_logs`
    # as per your insert statement.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS check_post_logs (
            id SERIAL PRIMARY KEY, -- Use SERIAL for auto-incrementing integer in PostgreSQL
            VEHICLE_NUMBER VARCHAR(50), -- Changed to VARCHAR as vehicle numbers often contain letters
            DRIVER_ID INT,
            OFFICER_ID INT,
            TIME_STAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            STATUS VARCHAR(20),
            DRUGS_RELATED_STOP BOOLEAN -- Added this column based on your insert loop
        )
    """)
    print("Table 'check_post_logs' created successfully (or already exists).")


    # officers table - This looks mostly correct for PostgreSQL
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS officers (
            officer_id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            post_location VARCHAR(100),
            shift_time VARCHAR(50)
        )
    """)
    print("Table 'officers' created successfully (or already exists).")

    # --- Insert Vehicle Data ---
    # Issue 3: `df` is not defined. You need to load your data into a pandas DataFrame.
    # Assuming `df` is a pandas DataFrame with columns:
    # 'VEHICLE_NUMBER', 'DRIVER_ID', 'OFFICER_ID', 'TIME_STAMP', 'STATUS', 'drugs_related_stop'

    # Create a dummy DataFrame for demonstration purposes
    data = {
        'VEHICLE_NUMBER': ['KA01AB1234', 'DL05CD5678', 'MH12EF9012', 'KA03GH3456','KA04IJ7890', 'DL08KL2345'],
        # Assuming VEHICLE_NUMBER is a string, as it often contains letters and numbers     
        # Assuming DRIVER_ID and OFFICER_ID are integers    
        'DRIVER_ID': [101, 102, 103, 104, 105, 106],
        # Assuming DRIVER_ID and OFFICER_ID are integers
        'OFFICER_ID': [1, 2, 1, 3, 2, 1],
        'TIME_STAMP': ['2023-01-15 10:00:00', '2023-01-15 11:30:00', '2023-01-16 09:00:00','2023-01-16 12:00:00', '2023-01-17 08:30:00', '2023-01-17 14:45:00'],
        # Assuming TIME_STAMP is a string in 'YYYY-MM-DD HH:MM:SS' format
        'STATUS': ['OK', 'STOPPED', 'WARNING','OK', 'STOPPED', 'WARNING'],
        # Assuming STATUS is a string, e.g., 'OK', 'STOPPED', 'WARNING'
        'drugs_related_stop': [False, True, False, True, False, True]
        # Assuming drugs_related_stop is a boolean indicating if the stop was drug-related
    }
    df = pd.DataFrame(data)

    print("\nInserting vehicle data...")
    for _, row in df.iterrows():
        try:
            # Issue 4: The insert statement was trying to insert into `traffic_stops`
            # but the table definition was `check_post_logs`. Also, the column
            # `drugs_related_stop` was in your try block but not in the INSERT statement.
            # I've added it to the `check_post_logs` table and the INSERT statement.
            cursor.execute("""
                INSERT INTO check_post_logs (VEHICLE_NUMBER, DRIVER_ID, OFFICER_ID, TIME_STAMP, STATUS, DRUGS_RELATED_STOP)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (row['VEHICLE_NUMBER'], row['DRIVER_ID'], row['OFFICER_ID'],
                  row['TIME_STAMP'], row['STATUS'], bool(row['drugs_related_stop'])))
        except Exception as e:
            print(f"Error inserting row {row['VEHICLE_NUMBER']}: {e}")

    print("Vehicle data insertion process completed.")


    # --- Insert Officer Data ---
    officers = [
        ('John Doe', 'Downtown', 'morning shift'),
        ('Jane Smith', 'Uptown', 'night shift'),
        ('Mike Johnson', 'Midtown', 'evening shift'),
        ('Emily Davis', 'Westside', 'morning shift'),
        ('Chris Brown', 'Eastside', 'night shift'),
        ('Sarah Wilson', 'Northside', 'evening shift')
    ]

    print("\nInserting officer data...")
    for officer in officers:
        try:
            cursor.execute("""
                INSERT INTO officers (name, post_location, shift_time)
                VALUES (%s, %s, %s)
            """, officer)
        except Exception as e:
            print(f"Error inserting officer {officer[0]}: {e}")
    print("Officer data insertion process completed.")

    # --- Commit and Close ---
    connection.commit()
    print("\nAll data committed successfully into PostgreSQL database.")

except psycopg2.Error as e:
    print(f"Database connection error: {e}")
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Database connection closed.")