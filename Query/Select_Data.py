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
    'database': 'DEV_TheBBCdata'
}

X = pass_query('''SELECT * FROM story;''',config)
print ("Story")
for item in X:
    print (item)

X = pass_query('''SELECT * FROM storyName LIMIT 20;''',config)
print ("StoryName")
for item in X:
    print (item)


X = pass_query('''SELECT * FROM topTen LIMIT 20 ;''',config)
print ("Top Ten")
for item in X:
    print (item)


X = pass_query('''SELECT * FROM topics;''',config)
print ("topics")
for item in X:
    print (item)


X = pass_query('''SELECT * FROM storyToTopic LIMIT 20;''',config)
print ("storyToTopic")
for item in X:
    print (item)

#X = pass_query('''SELECT * FROM storyName ;''',config)

X = pass_query('''SELECT * FROM author;''',config)
print ("Author")
for item in X:
    print (item)



X = pass_query('''SELECT * FROM authorToStory ;''',config)
print ("Author to Story ")
for item in X:
    print (item)

X = pass_query('''SELECT Count(*) FROM story ;''',config)
print ("Count Stories  ")
for item in X:
    print (item)

X = pass_query('''SELECT Count(*) FROM author ;''',config)
print ("Count Authors  ")
for item in X:
    print (item)


X = pass_query('''SELECT Count(*) FROM story WHERE hasStoryBeenProcessed = 1 ;''',config)
print ("Processed Stories")
for item in X:
    print (item)