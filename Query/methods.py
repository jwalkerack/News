
def create_database(config ,databaseName):
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    if cnx.is_connected():
        try:
            print("Connection is Open !!")
            cursor = cnx.cursor()
            dbQuery =  f"CREATE DATABASE IF NOT EXISTS {databaseName};"
            cursor.execute(dbQuery)
            print (f"The {databaseName} has been created")
        except OSError as e:
                print (f"The {databaseName} creation contained errors - {e}")

        cursor.close()
        cnx.close()


def drop_database(config ,databaseName):
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    if cnx.is_connected():
        try:
            print("Connection is Open !!")
            cursor = cnx.cursor()
            dbQuery =  f"DROP DATABASE {databaseName};"
            cursor.execute(dbQuery)
            print (f"The {databaseName} has been dropped")
        except OSError as e:
                print (f"The {databaseName} droping contained errors - {e}")

        cursor.close()
        cnx.close()


def create_tables(database,config ,tables):
    import re
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    if cnx.is_connected():
        print("Connection is Open !!")
        cursor = cnx.cursor()
        cursor.execute(f"USE {database};")
        for table in tables:
            try:
                cursor.execute(table)
                cnx.commit()
                pattern = r"CREATE TABLE IF NOT EXISTS\s+(\w+)\s*\("
                match = re.search(pattern, table)
                if match:
                    table_name = match.group(1)

                else:
                    table_name = "Table name not found"
                print (f"The {table_name} has been created")
            except OSError as e:
                print (f"The {table} creation contained errors - {e}")

        cursor.close()
        cnx.close()














config = {
    'user': 'root',
    'password': 'example',  # Use your root password here
    'host': 'localhost',
    'port': 3307,
}



#drop_database(config ,'Football')
#create_tables(database,config ,tables)