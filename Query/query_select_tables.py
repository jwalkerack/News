def pass_query(Q,config):
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    if cnx.is_connected():
        cursor = cnx.cursor()
        try:
            cursor.execute(Q)
            Data = cursor.fetchall()
        except:
            pass
            Data = None
    return Data


config = {
    'user': 'root',
    'password': 'example',  # Use your root password here
    'host': 'localhost',
    'port': 3307,
    'database': 'TheBBCdata'
}

X = pass_query('''SELECT * FROM story ;''',config)
print ("Story")
for item in X:
    print (item)

X = pass_query('''SELECT * FROM storyName ;''',config)
print ("StoryName")
for item in X:
    print (item)


X = pass_query('''SELECT * FROM topTen ;''',config)
print ("Top Ten")
for item in X:
    print (item)


#X = pass_query('''SELECT * FROM storyName ;''',config)

#for item in X:
    #print (item)