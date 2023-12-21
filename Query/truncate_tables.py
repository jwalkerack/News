config = {
    'user': 'root',
    'password': 'example',  # Use your root password here
    'host': 'localhost',
    'port': 3307,
    'database': 'DEV_TheBBCdata'
}


def flush_table(table_name: str):
    import mysql.connector

    # Using string formatting to insert the table name into the query
    # Be cautious about SQL injection if table_name comes from user input
    delete_query = f"DELETE FROM {table_name}"

    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()

    try:
        cursor.execute(delete_query)
        cnx.commit()
    except Exception as e:
        print(e)
        handle_all_exceptions(e)
    finally:
        cnx.close()

    print(f"All data removed from {table_name} table.")


flush_table("author")
flush_table("authorToStory")