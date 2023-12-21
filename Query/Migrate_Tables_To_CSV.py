import csv
import mysql.connector

def save_to_csv(cursor, file_path):
    with open(file_path, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for row in cursor:
            writer.writerow(row)

def pass_query(Q, config):
    cnx = mysql.connector.connect(**config)
    cursor = None
    if cnx.is_connected():
        cursor = cnx.cursor()
        try:
            cursor.execute(Q)
        except Exception as e:
            print("An error occurred:", e)
            cursor = None
    return cursor, cnx  # Return both cursor and connection

config = {
    'user': 'root',
    'password': 'example',  # Use your root password here
    'host': 'localhost',
    'port': 3307,
    'database': 'DEV_TheBBCdata'
}

cursor, cnx = pass_query('''SELECT * FROM storyToTopic;''', config)  # Get both cursor and connection
if cursor:
    save_to_csv(cursor, r'C:\Users\44756\Documents\General\BBC\Version2\storyToTopic_EXTRACT.csv')
    cursor.close()  # Close the cursor
    cnx.close()     # Close the connection
else:
    print("No data to save or error occurred")

cursor, cnx = pass_query('''SELECT * FROM story;''', config)  # Get both cursor and connection
if cursor:
    save_to_csv(cursor, r'C:\Users\44756\Documents\General\BBC\Version2\story_EXTRACT.csv')
    cursor.close()  # Close the cursor
    cnx.close()     # Close the connection
else:
    print("No data to save or error occurred")

cursor, cnx = pass_query('''SELECT * FROM storyName;''', config)  # Get both cursor and connection
if cursor:
    save_to_csv(cursor, r'C:\Users\44756\Documents\General\BBC\Version2\storyName_EXTRACT.csv')
    cursor.close()  # Close the cursor
    cnx.close()     # Close the connection
else:
    print("No data to save or error occurred")

cursor, cnx = pass_query('''SELECT * FROM topics;''', config)  # Get both cursor and connection
if cursor:
    save_to_csv(cursor, r'C:\Users\44756\Documents\General\BBC\Version2\topics_EXTRACT.csv')
    cursor.close()  # Close the cursor
    cnx.close()     # Close the connection
else:
    print("No data to save or error occurred")

cursor, cnx = pass_query('''SELECT * FROM storyToTopic;''', config)  # Get both cursor and connection
if cursor:
    save_to_csv(cursor, r'C:\Users\44756\Documents\General\BBC\Version2\storyToTopic_EXTRACT.csv')
    cursor.close()  # Close the cursor
    cnx.close()     # Close the connection
else:
    print("No data to save or error occurred")

cursor, cnx = pass_query('''SELECT * FROM author;''', config)  # Get both cursor and connection
if cursor:
    save_to_csv(cursor, r'C:\Users\44756\Documents\General\BBC\Version2\author_EXTRACT.csv')
    cursor.close()  # Close the cursor
    cnx.close()     # Close the connection
else:
    print("No data to save or error occurred")

cursor, cnx = pass_query('''SELECT * FROM authorToStory;''', config)  # Get both cursor and connection
if cursor:
    save_to_csv(cursor, r'C:\Users\44756\Documents\General\BBC\Version2\authorToStory_EXTRACT.csv')
    cursor.close()  # Close the cursor
    cnx.close()     # Close the connection
else:
    print("No data to save or error occurred")