from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests


story = """
    CREATE TABLE IF NOT EXISTS story (
        id INT AUTO_INCREMENT PRIMARY KEY,
        url VARCHAR(255),
        author VARCHAR(255),
        story_created_on TIMESTAMP,
        story_last_updated VARCHAR(255),
        hasStoryBeenProcessed TINYINT(1) DEFAULT 0 ,
        hasAuthorBeenProcessed TINYINT(1) DEFAULT 0 ,
        storySourcedVia VARCHAR(50),
        createdDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        lastmodifiedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
"""

storyName = """
    CREATE TABLE IF NOT EXISTS storyName (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(255) ,
        storyId  INT,
        FromDateTime  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        ToDateTime    TIMESTAMP,
        FOREIGN KEY fk_storyId(storyId) REFERENCES story(id)       
    );
"""


topTen = """
    CREATE TABLE IF NOT EXISTS topTen (
        id INT AUTO_INCREMENT PRIMARY KEY,
        storyUrl VARCHAR(255) , 
        position INT  ,
        extractionDate TIMESTAMP
    );"""

topics = """
    CREATE TABLE IF NOT EXISTS topics (
        id INT AUTO_INCREMENT PRIMARY KEY,
        topicUrl VARCHAR(255),
        topicName VARCHAR(255),
        hasTopicBeenProcessed TINYINT(1) DEFAULT 0
    );"""

storyToTopic = """
    CREATE TABLE IF NOT EXISTS storyToTopic (
        id INT AUTO_INCREMENT PRIMARY KEY,
        topicId INT,
        storyId INT
    );"""

author = """
    CREATE TABLE IF NOT EXISTS author (
        id INT AUTO_INCREMENT PRIMARY KEY,
        longName VARCHAR(255)
    );"""


authorToStory = """
    CREATE TABLE IF NOT EXISTS authorToStory (
        id INT AUTO_INCREMENT PRIMARY KEY,
        authorId INT,
        storyId INT
    );"""


Tables = [story,storyName,topTen, topics, storyToTopic,author,authorToStory]

config = {
    'user': 'root',
    'password': 'example',  # Use your root password here
    'host': 'mariadb',
    'port': 3306,
    'database': 'DEV_TheBBCdata'
}



def handle_all_exceptions(exception):
    ### This function is the main erroring handling. Which is used in any function that has an interaction with the DB
    import inspect
    # Set up logging on first exception handled
    if not logging.getLogger().hasHandlers():
        setup_logging()

    # Get the name of the current function
    current_function = inspect.currentframe().f_back.f_code.co_name

    if isinstance(exception, ProgrammingError):
        logging.error("%s - Programming Error: %s", current_function, exception)
    elif isinstance(exception, DataError):
        logging.error("%s - Data Error: %s", current_function, exception)
    elif isinstance(exception, OperationalError):
        logging.error("%s - Operational Error: %s", current_function, exception)
    elif isinstance(exception, Error):
        logging.error("%s - General Database Error: %s", current_function, exception)
    else:
        # Log general exceptions
        logging.error("%s - Unexpected Error: %s", current_function, exception)

import logging
from mysql.connector import Error, ProgrammingError, DataError, OperationalError
def setup_logging():
    """ Sets up the logging configuration. """
    logging.basicConfig(filename=r'C:\Users\44756\PycharmProjects\Airflow\csv\all_errors.log',
                        filemode='a',  # Append to the log file
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.ERROR)

def create_tables(database,config ,tables):
    ## This Function Takes a series of SQL table statements , which are defined above and creates these Tables
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

def create_database(databaseName):
    ## Create the Database
    config = {
        'user': 'root',
        'password': 'example',  # Use your root password here
        'host': 'mariadb',
        'port': 3306
    }
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
        finally:
            cnx.close()


def does_story_title_exist(storyId ,storyTitle):
    ## Used to determine if a story title exist in relation to a story id in the storyname table
    select_count_url = "SELECT COUNT(id) FROM storyName WHERE storyId = %s and title = %s "
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(select_count_url, (storyId,storyTitle))
        result = cursor.fetchall()
        count = result[0][0]
        if count == 0:
            idExist = False
        else:
            idExist = True
        return idExist
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()
def doesStoryUrlExist(storyUrl):
    ## Used to determine of a story URL exist in the story table
    select_count_url = "SELECT COUNT(id) FROM story WHERE url = %s"
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(select_count_url, (storyUrl,))
        result = cursor.fetchall()
        count = result[0][0]
        if count == 0:
            idExist = False
        else:
            idExist = True
        print (count)
        return idExist
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()


def count_of_TopicUrl(TopicUrl):
    ## Performs a count of the occurance of the Topic URL in the Topics Table
    ## This acts as proxy to determine if the Topic Exists or not
    search_for_topic = "Select COUNT(id) FROM topics WHERE  topicUrl = %s"
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(search_for_topic, (TopicUrl,))
        result = cursor.fetchall()
        count = result[0][0]
        return count
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()

def count_of_story_topic(topicId,storyId):
    ## Performs a count of the occurance of the Topic ID and Story ID in the storyTopic Table
    ## This acts as a proxy to determine if this already exist
    search_for_topic_story = "Select COUNT(id) FROM storyToTopic WHERE  topicId = %s AND storyId = %s "
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(search_for_topic_story, (topicId,storyId))
        result = cursor.fetchall()
        count = result[0][0]
        return count
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()


def retrive_story_id(storyUrl):
    ## Takes a story URL and looks it up in the story table and returns the ID related to that URL
    import mysql.connector
    select_count_url = "SELECT id FROM story WHERE url = %s"
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(select_count_url, (storyUrl,))
        result = cursor.fetchall()
        storyId = result[0][0]
        return storyId
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()

def retrive_topicId_via_url(topicUrl):
    ## Takes a story URL and looks it up in the story table and returns the ID related to that URL
    import mysql.connector
    select_count_url = "SELECT id FROM topics WHERE topicUrl = %s"
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(select_count_url, (topicUrl,))
        result = cursor.fetchall()
        topicId = result[0][0]
        return topicId
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()

def insert_story(storyUrl,source):
    ## Used to insert a story into the story table
    ## THIS Contains no logic to determine if the story url already exist.
    ## Understanding that upstream code has determined that it does not exist
    insert_story = "INSERT INTO story (url,storySourcedVia) VALUES (%s,%s)"
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(insert_story, (storyUrl, source))
        cnx.commit()
        cnx.close()
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()
def insert_StoryName(storyTitle, storyId):
    ## used to insert story name into storyname
    insert_storyName = "INSERT INTO storyName (title,storyId) VALUES (%s,%s)"
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(insert_storyName, (storyTitle, storyId))
        cnx.commit()
        cnx.close()
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()

def insert_story_topic_mapping(topicId,storyId):
    ## Insert into to StoryToTopic
    import mysql.connector
    insert_mapping_data = "INSERT INTO storyToTopic (topicId,storyId) VALUES (%s,%s)"
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(insert_mapping_data, (topicId, storyId))
        cnx.commit()
        cnx.close()
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()

def insert_topic(topicUrl,topicName):
    ## Insert into Topic
    import mysql.connector
    insert_topic_data = "INSERT INTO topics (topicUrl,topicName) VALUES (%s,%s)"
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(insert_topic_data, (topicUrl, topicName))
        cnx.commit()
        cnx.close()
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()


def update_storyName_date(storyId):
    ## Update to the storyName , this is for the purposes of archiving previous Name
    # Potential issue with as story titles , maybe access inconsistant temporal order , leading to inconsistancies
    import mysql.connector
    update_date_statement = """UPDATE storyName SET ToDateTime = CURRENT_TIMESTAMP WHERE storyId = %s AND ToDateTime IS NULL"""
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(update_date_statement, (storyId,))
        result = cursor.fetchall()
        storyId = result[0][0]
        return storyId
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()

def update_Story(data_row):
    ## Updates the story table using the data sourced from the request to the storypage
    ## Closes of the story by setting the hasStorybeenProcessed to 1.
    print (data_row)
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    update_previous_title_query = f"""UPDATE story SET author = %s,story_created_on = %s,story_last_updated = %s,hasStoryBeenProcessed = 1 WHERE id = %s"""
    try:
        cursor.execute(update_previous_title_query, (data_row[1], data_row[2], data_row[3], data_row[0]))
        cnx.commit()
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()

def update_topic_has_been_processed(topicId):
    #Updates the Topic Table after the Topic has been process
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    update_hasTopicBeenProcessed = """ UPDATE topics SET hasTopicBeenProcessed = 1 WHERE id = %s """
    try:
        cursor.execute(update_hasTopicBeenProcessed, (topicId,))
        cnx.commit()
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()

##### In relation to AUTHOR Text field processing

def count_of_author_text_in_author(author:str):
    ## This will take a author string like "John Wayne" and look inside the
    #  The author_ref Table to determine if it exist.
    # If the string does not exist it will return 0
    search_for_author = "Select COUNT(id) FROM author WHERE  longName = %s"
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(search_for_author, (author,))
        result = cursor.fetchall()
        count = result[0][0]
        return count
    except Exception as e:
        print("No you have error here ")
        print(e)
        handle_all_exceptions(e)
    finally:
        cnx.close()

def select_twenty_table(table_name: str):
    import mysql.connector
    select_author = f"SELECT * FROM {table_name} LIMIT 20"
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(select_author)
        result = cursor.fetchall()
        return result
    except Exception as e:
        print (e)
        handle_all_exceptions(e)
    finally:
        cnx.close()

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



def insert_new_correspondant_into_author(author_text):
    import mysql.connector
    insert_author = "INSERT INTO author (longName) VALUES (%s)"
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(insert_author, (author_text,))
        cnx.commit()
        cnx.close()
    except Exception as e:
        print (e)
        handle_all_exceptions(e)
    finally:
        cnx.close()


def retrive_author_id(author_text):
    ## This function takes an author_text and then passes back the id
    ## This could actually overide the functionality of the count ... as it can also be used for infer
    # For now its being left as two things ..
    import mysql.connector
    select_query = "SELECT id FROM author WHERE longName = %s"
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(select_query, (author_text,))
        result = cursor.fetchall()
        authorId = result[0][0]
        return authorId
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()


def count_authorId_with_storyId(authorId, storyId):
    ## Returns a count for the authorId & StoryId to determine if the mapping for author and story has been generated
    ## If it does not exist , this will return 0 , if it does it will return 1 . It should not be possible for the relationship
    ## To be defined multiply times .. This data input is controlled in the insert function
    count_storyAuthorMapping = "Select COUNT(id) FROM authorToStory WHERE  authorId = %s AND storyId = %s "
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(count_storyAuthorMapping, (authorId, storyId))
        result = cursor.fetchall()
        count = result[0][0]
        return count
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()


def insert_mapping_into_authorStory(authorId, storyId):
    ### This is used when it has already been determined that the mapping does not exist
    import mysql.connector
    insert_author = "INSERT INTO authorToStory (authorId,storyId) VALUES (%s,%s)"
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(insert_author, (authorId,storyId))
        cnx.commit()
        cnx.close()
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()


def update_author_processed_state(storyId):
    ## This function updates the story table hasAuthorBeenProcessed
    ## This acts a control point after the structured authors have been processed from author field.
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    update_AuthorProcessed = f"""UPDATE story SET hasAuthorBeenProcessed = 1 WHERE id = %s"""
    try:
        cursor.execute(update_AuthorProcessed, (storyId,))
        cnx.commit()
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()


def retrive_count_of_stories():
    select_count = "SELECT COUNT(id) FROM story WHERE hasStoryBeenProcessed = 1 AND author != ''"
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(select_count)
        result = cursor.fetchall()
        count = result[0][0]
        return count
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()

def view_storyId_row_in_story(storyId):
    select_storyRow  = "SELECT * FROM story WHERE id  = %s"
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(select_storyRow ,(storyId,))
        result = cursor.fetchall()
        return result
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()


def retrive_stories_with_author_that_have_been_processed():
    ### This function retrives the authors - ready to be processed
    select_count = "SELECT id , url , author  FROM story WHERE hasAuthorBeenProcessed = 0 AND author != ''"
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(select_count)
        result = cursor.fetchall()
        return result
    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cnx.close()

def extract_author_names_from_pulled(author_text):
    import re
    # Remove the 'By ' prefix if it exists
    author_text = author_text.replace('By ', '')

    # Define a regular expression pattern to split the string
    # This pattern looks for commas, 'and', or '&' as delimiters.
    pattern = r',|\band\b|\&'

    # Split the author_text using the defined pattern
    authors = re.split(pattern, author_text)

    # Strip whitespace from each author name and filter out any empty strings
    author_names = [author.strip() for author in authors if author.strip()]

    return author_names


def Generate_Soup(URL):
    import requests
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
    from bs4 import BeautifulSoup as bs
    try:
        make_request = requests.get(URL, headers=headers)
        if make_request.status_code == 200:
            make_soup = [bs(make_request.text, "html.parser"), True]
        else:
            make_soup = [bs(make_request.text, "html.parser"), make_request.status_code]
        return make_soup
    except requests.exceptions.RequestException as e:
        print(f'Error: {e}')
        make_soup = [0, False]
        return make_soup

def extract_time_elements(soup):
    try:
        time_element = soup.find('time', {'data-testid': 'timestamp'})
        if time_element is not None:
            datetimex = time_element.get('datetime')
            NewTime = datetime.fromisoformat(datetimex.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
            text = time_element.get_text().strip()
        else:
            print("Time element not found")
            NewTime = ""
            text = ""
        return [NewTime, text]
    except Exception as e:
        print(f'Error: {e}')
        return ["", ""]

def extract_contributor_name(soup):
    import re
    # Using a regular expression to match the class
    contributor_div = soup.find('div', class_=re.compile(".*TextContributorName.*"))
    if contributor_div:
        return contributor_div.get_text(strip=True)
    else:
        return ""

def extract_Topics(soup):
    links_with_text = []
    topic_list_div = soup.find('div', {'data-component': 'topic-list'})

    if topic_list_div:
        for link in topic_list_div.find_all('a', href=True):
            if "/topics/" in link['href']:
                links_with_text.append({"url": link['href'], "name": link.get_text(strip=True)})

    return links_with_text




def generate_request(url):
    import json
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }
    try:
        make_request = requests.get(url, headers=headers)
        make_request_data = {
            'url': url,
            'headers': headers,
            'status_code': make_request.status_code,
            'response_text': make_request.text if make_request.status_code == 200 else None
        }

        # Convert the make_request_data dictionary to JSON
        make_request_json = json.dumps(make_request_data)
    except:
        make_request_data = {
            'url': url,
            'headers': headers,
            'status_code': 999,
            'response_text': None
        }
        make_request_json = json.dumps(make_request_data)

    return make_request_json

# Function to extract most read news from BBC
def process_soup(ti):
    Data = ti.xcom_pull(task_ids='request_BBC_News')
    import json
    import datetime
    from bs4 import BeautifulSoup as bs
    json_data = json.loads(Data)
    Xdata = []
    if json_data['status_code'] == 200:
        print('Yes')
        soup = bs(json_data['response_text'])
        Order = 1
        current_datetime = datetime.datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        for item in soup.select('.nw-c-most-read__items li'):
            title = item.find("span", class_="gs-c-promo-heading__title").text
            href = item.find("a")["href"]
            row = [Order, title, href, formatted_datetime]
            print (row)
            Xdata.append(row)
            Order += 1
    return Xdata


def write_stories_sourced_via_topten(ti):
    # Takes the data that has been extracted from the TopTen element in the BBC news page
    ## Then writes the to the Story and Story name tables in the Database
    config = {
        'user': 'root',
        'password': 'example',  # Use your root password here
        'host': 'mariadb',
        'port': 3306,
        'database': 'DEV_TheBBCdata'
    }

    data = ti.xcom_pull(task_ids='generate_top_ten_stories')
    for story in data:
        storyUrl = story[2]
        storyText = story[1]
        if doesStoryUrlExist(storyUrl) == True:
            storyId = retrive_story_id(storyUrl)
            if does_story_title_exist(storyId ,storyText) == False:
                update_storyName_date(storyId)
                insert_StoryName(storyText, storyId)

        else:
            insert_story(storyUrl,"TopTen")
            storyId = retrive_story_id(storyUrl)
            insert_StoryName(storyText, storyId)

def AF_DB_insert_topics_sourced_via_storiesTT(ti):
    ### The Purpose of this to write the Topic data that was retrived in the processing of the stories
    ### Writing this data to the Topic and story/topic tables
    DataDict = ti.xcom_pull(task_ids='generate_story_extract_json')
    for item in DataDict:
        storyId = (item)
        topics = DataDict[storyId]['topics']
        for topic in topics:
            topicName = topic['name']
            topicUrl = topic['url']
            if count_of_TopicUrl(topicUrl) == 0:
                ## The Topic URL does not exist and needs to be created
                ## The Topic ID is then retrived and the story/mapping is generated
                insert_topic(topicUrl, topicName)
                topicId = retrive_topicId_via_url(topicUrl)
                insert_story_topic_mapping(topicId, storyId)
            else:
                ## The Topic Does Exist , Access the topic ID , check if story mapping exist. If it does not write
                topicId = retrive_topicId_via_url(topicUrl)
                if count_of_story_topic(topicId,storyId) == 0:
                    insert_story_topic_mapping(topicId, storyId)


def generateStoriesToBeRequested():
    ## Runs a query into the stories table to determine which storys need to be request
    ## It returns the story URLs and the database ID for these URLs
    config = {
        'user': 'root',
        'password': 'example',  # Use your root password here
        'host': 'mariadb',
        'port': 3306,
        'database': 'DEV_TheBBCdata'
    }

    import mysql.connector
    cnx = mysql.connector.connect(**config)
    if cnx.is_connected():
        cursor = cnx.cursor()
        try:
            cursor.execute('''SELECT id , url FROM story WHERE hasStoryBeenProcessed = 0 and storySourcedVia = 'TopTen' ;''')
            Data = cursor.fetchall()
        except Exception as e:
            handle_all_exceptions(e)
    forProcessing = []
    for item in Data:
        storyId = item[0]
        storyUrl = item[1]
        Row = [storyId, storyUrl]
        forProcessing.append(Row)
    return forProcessing

def generate_story_extract_json(ti):
    ## This Takes the Story URLs and then makes a call to each of the URLS.
    ## It then converts the reponse into B4 Soup
    ## The soup is then processed to extract fields that relate to the story
    ## Story Author , Story Created on , Story Topics
    ## This Data is then stored in a dictionary , using the Story Database ID as the Key
    data = ti.xcom_pull(task_ids='generate_stories_to_be_requested')
    DataDict = {}

    ### Task you need to generate the Data
    for item in data:
        mainUrl = "https://www.bbc.co.uk/"
        storyId = item[0]
        storyUrl = item[1]
        request_url = mainUrl + storyUrl
        call_page = Generate_Soup(request_url)
        if call_page[1] == True:
            ### You Have a Page
            time_elements = extract_time_elements(call_page[0])
            author = extract_contributor_name(call_page[0])
            topics = extract_Topics(call_page[0])
            DataDict[storyId] = {"author": author,
                                 "story_created_on": time_elements[0],
                                 "story_updated_on": time_elements[1],
                                 "topics" : topics}
    return DataDict

def AF_process_updates_to_story(ti):
    ## The Purpose of this Function is process the Data that was generated when calling the story URLS
    ## This Function only deals with updating the story table attributes
    ## A seperate function deals with the story/topic relationships
    X = ti.xcom_pull(task_ids='generate_story_extract_json')

    def access_update_data(Dict, Key):
        Author = Dict[Key]["author"]
        OrgDate = Dict[Key]["story_created_on"]
        LastUp = Dict[Key]["story_updated_on"]
        topics = Dict[Key]["topics"]
        return [Key, Author, OrgDate, LastUp, topics]
    for item in X:
        GetData = access_update_data(X, item)
        update_Story(GetData)

def AF_DB_query_Topics_To_Be_Requested():
    # This will query the Topics Table and take the first 2 topics that need to be process.
    # The limit is to control the amount of stories that come into the system downstream
    config = {
        'user': 'root',
        'password': 'example',  # Use your root password here
        'host': 'mariadb',
        'port': 3306,
        'database': 'DEV_TheBBCdata'
    }
    import mysql.connector
    cnx = mysql.connector.connect(**config)
    if cnx.is_connected():
        cursor = cnx.cursor()
        try:
            cursor.execute('''SELECT id , topicUrl FROM topics WHERE hasTopicBeenProcessed = 0 LIMIT 2;''')
            Data = cursor.fetchall()
        except Exception as e:
            handle_all_exceptions(e)


    forProcessing = []
    for item in Data:
        topicId = item[0]
        topicUrl = item[1]
        Row = [topicId, topicUrl]
        forProcessing.append(Row)
    return forProcessing


def AF_request_extract_topic_pages(ti):
    D = {}
    X = ti.xcom_pull(task_ids='AF_DB_query_Topics_To_Be_Requested')
    for item in X:
        topicUrl = item[1]
        topicId  = item[0]
        get_stories_under_topics(topicId,topicUrl, D)

    return D

def get_stories_under_topics(topicId ,topicUrlSnippet, DataCollection):
    def Generate_Soup(URL):
        import requests
        headers = {'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
        from bs4 import BeautifulSoup as bs
        try:
            make_request = requests.get(URL,headers=headers)
            if make_request.status_code == 200:
                make_soup = [bs(make_request.text, "html.parser"), True]
            else:
                make_soup = [bs(make_request.text, "html.parser"), make_request.status_code]
            return make_soup
        except requests.exceptions.RequestException as e:
            make_soup = [0 , False]
            return make_soup

            print(f'Error: {e}')

    def Get_Pagination_Pages(soup):
        import re
        pagination_container = soup.find('div', class_=re.compile(".*NumberedPagesButtonsContainer*."))
        try:
            page_items = pagination_container.find_all('li')
            pages = []
            isMultiPage = True
            for item in page_items:
                a_tag = item.find('a')
                if a_tag:
                    url = a_tag['href']
                    page_number = item.find('div', class_=re.compile(".*StyledButtonContent.*")).get_text()
                    try:
                        page_number = int(page_number)
                        pages.append(page_number)
                    except:
                        pass
        except:
            pages = None
            isMultiPage = False

        return [isMultiPage, pages]

    def pagesRange(MaxPage):
        pages_list = list(range(2, MaxPage + 1))
        stringNum = ["?page=" + str(i) for i in pages_list]
        return stringNum

    def extract_articles_url_titles(soup):
        import re
        articles_data = []
        container = soup.find('div', class_=re.compile(".*SimpleGridContainer*."))
        if container:
            # Find
            articles = container.find_all('div', attrs={'type': 'article'})

            for article in articles:
                # Find the story link
                link_tag = article.find('a', class_=re.compile(".*PromoLink*."))
                link = link_tag.get('href') if link_tag else None

                # Find the story title
                title_tag = article.find(class_=re.compile(".*PromoHeadline*."))
                title = title_tag.get_text() if title_tag else None
                articles_data.append({"storyUrl": link, "storyTitle": title})
        return articles_data
        ###  Run Processes

    main_url = "https://www.bbc.co.uk"
    topicUrl = main_url + topicUrlSnippet
    make_request = Generate_Soup(topicUrl)
    if make_request[1] == True:
        containedPages = Get_Pagination_Pages(make_request[0])
        page_key_counter = 0
        if containedPages[0] != False:
            paginationPages = max(containedPages[1])
            pagesToProcess = pagesRange(paginationPages)
            ######## First Time Its Called ....
            articles = extract_articles_url_titles(make_request[0])
            DataCollection[topicId] = {page_key_counter: articles}
            page_key_counter += 1
            for pageNumber in pagesToProcess:
                topic_page_url = topicUrl + pageNumber
                make_topic_page_call = Generate_Soup(topic_page_url)
                if make_topic_page_call[1] == True:
                    articles = extract_articles_url_titles(make_topic_page_call[0])
                    DataCollection[topicId][page_key_counter] = articles
                    page_key_counter += 1
        else:
            articles = extract_articles_url_titles(make_request[0])
            DataCollection[topicId] = {page_key_counter: articles}

    return DataCollection

def AF_DB_Q_stories_to_req_via_topics():
    import mysql.connector
    query_statement = '''SELECT id , url FROM story WHERE hasStoryBeenProcessed = 0 and storySourcedVia = 'Topic' ORDER BY RAND() LIMIT 100 ;'''
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    try:
        cursor.execute(query_statement)
        results = cursor.fetchall()
        stories_dict = {id: url for id, url in results}
        return stories_dict

    except Exception as e:
        handle_all_exceptions(e)
    finally:
        cursor.close()
        cnx.close()


#


def AF_request_enriching_stories_via_topics(ti):
    import time
    ## This Function is requesting story URLS and saving data to then be updated in Database
    ## This specfically related to subset of stories that have been sourced from topics
    data = ti.xcom_pull(task_ids='AF_DB_Q_stories_to_req_via_topics')
    DataDict = {}

    ### Task you need to generate the Data
    for item in data:
        storyId = item
        storyUrl = data[item]
        mainUrl = "https://www.bbc.co.uk/"
        request_url = mainUrl + storyUrl
        call_page = Generate_Soup(request_url)
        if call_page[1] == True:
            ### You Have a Page
            time_elements = extract_time_elements(call_page[0])
            author = extract_contributor_name(call_page[0])
            topics = extract_Topics(call_page[0])
            DataDict[storyId] = {"author": author,
                                 "story_created_on": time_elements[0],
                                 "story_updated_on": time_elements[1],
                                 "topics" : topics}
        time.sleep(3)
    return DataDict

def AF_DB_update_stories_via_topics(ti):
    data = ti.xcom_pull(task_ids='AF_request_enriching_stories_via_topics')
    for story in data:
        storyId = story
        storyObject =  data[story]
        author = storyObject['author']
        story_created_on = storyObject['story_created_on']
        story_updated_on = storyObject['story_updated_on']
        topics_tied_story = storyObject['topics']
        ### The Process that Will Update the Story Attributes
        row = [storyId ,author,story_created_on,story_updated_on]
        update_Story(row)
        ### The Process that Will insert the Topics
        for topic in topics_tied_story:
            topicName = topic['name']
            topicUrl = topic['url']
            if count_of_TopicUrl(topicUrl) == 0:
                insert_topic(topicUrl, topicName)
                topicId = retrive_topicId_via_url(topicUrl)
                insert_story_topic_mapping(topicId, storyId)
            else:
                ## The Topic Does Exist , Access the topic ID , check if story mapping exist. If it does not write
                topicId = retrive_topicId_via_url(topicUrl)
                if count_of_story_topic(topicId,storyId) == 0:
                    insert_story_topic_mapping(topicId, storyId)


def AF_DB_run_author_write_process():
    ## This Function calls the sub functions , which take data held in the Author Field in the story
    ## This data is processed , the authors are stored in the author table. The relationships between the author and story
    ## In the author /mapping table
    author_data = retrive_stories_with_author_that_have_been_processed()
    for author in author_data:
        storyId = author[0]
        authorText = author[2]
        extract_authors = extract_author_names_from_pulled(authorText)
        for writer in extract_authors:
            print (storyId, writer )
            if count_of_author_text_in_author(writer) == 0:
                insert_new_correspondant_into_author(writer)
                authorId = retrive_author_id(writer)
                insert_mapping_into_authorStory(authorId, storyId)
            else:
                authorId = retrive_author_id(writer)
                if count_authorId_with_storyId(authorId, storyId) == 0:
                    insert_mapping_into_authorStory(authorId, storyId)
        update_author_processed_state(storyId)



############ CUT LINE ###################







# Function to generate soup object from URL


# Function to append data to CSV





def AF_DB_insert_stories_topics_Topics(ti):
    ## Takes the Data that was generated from the request to the topic pages
    ## This consists of Topic ID , story URL , story Title
    ## This loops through the data , writing elements when the do not exist
    data_in = ti.xcom_pull(task_ids='AF_request_extract_topic_pages')
    for topicId in data_in:
        for page in data_in[topicId]:
            for story in data_in[topicId][page]:
                storyUrl = story['storyUrl']
                storyTitle = story['storyTitle']
                ### doesStoryUrlExist from Global Function
                if doesStoryUrlExist(storyUrl) == True:
                    #### - retrive_story_id from Global Function
                    storyId = retrive_story_id(storyUrl)
                    if count_of_story_topic(topicId,storyId) == 0:
                        insert_story_topic_mapping(topicId, storyId)
                else:
                    #### insert_story - Global
                    insert_story(storyUrl,"Topic")
                    storyId = retrive_story_id(storyUrl)
                    insert_StoryName(storyTitle, storyId)
                    if count_of_story_topic(topicId,storyId) == 0:
                        insert_story_topic_mapping(topicId, storyId)
        update_topic_has_been_processed(topicId)








# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    '1.1_NEWS_Extraction',
    default_args=default_args,
    description='Extraction of Stories Data From BBC News',
    schedule=timedelta(minutes=10),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Task definitions
url = 'https://www.bbc.co.uk/news'  # Example URL




make_database = PythonOperator(
    task_id='make_database',
    op_kwargs={'config': config,'databaseName' : 'DEV_TheBBCdata'},
    python_callable=create_database,
    dag=dag)

make_tables = PythonOperator(
    task_id='make_tables',
    op_kwargs={'config': config,'database' : 'DEV_TheBBCdata','tables':Tables},
    python_callable=create_tables,
    dag=dag)


request_BBC_News = PythonOperator(
    task_id='request_BBC_News',
    python_callable=generate_request,
    op_kwargs={'url': url},
    dag=dag,
)

generate_news_topten = PythonOperator(
    task_id='generate_top_ten_stories',
    python_callable=process_soup,
    dag=dag,
)


write_extracted_story_data = PythonOperator(
    task_id='write_stories_sourced_via_topten',
    python_callable=write_stories_sourced_via_topten,
    dag=dag,
)

generate_story_List_to_be_request  = PythonOperator(
    task_id='generate_stories_to_be_requested',
    python_callable=generateStoriesToBeRequested,
    dag=dag,
)

generate_story_extract_json  = PythonOperator(
    task_id='generate_story_extract_json',
    python_callable=generate_story_extract_json,
    dag=dag,
)

AF_process_updates_to_story  = PythonOperator(
    task_id='AF_process_updates_to_story',
    python_callable=AF_process_updates_to_story,
    dag=dag,
)

AF_DB_insert_topics_sourced_via_storiesTT  = PythonOperator(
    task_id='AF_DB_insert_topics_sourced_via_storiesTT',
    python_callable=AF_DB_insert_topics_sourced_via_storiesTT,
    dag=dag,
)


AF_DB_query_Topics_To_Be_Requested  = PythonOperator(
    task_id='AF_DB_query_Topics_To_Be_Requested',
    python_callable=AF_DB_query_Topics_To_Be_Requested,
    dag=dag,
)



AF_request_extract_topic_pages  = PythonOperator(
    task_id='AF_request_extract_topic_pages',
    python_callable=AF_request_extract_topic_pages,
    dag=dag,
)


AF_DB_insert_stories_topics_Topics  = PythonOperator(
    task_id='AF_DB_insert_stories_topics_Topics',
    python_callable=AF_DB_insert_stories_topics_Topics,
    dag=dag,
)



AF_DB_Q_stories_to_req_via_topics  = PythonOperator(
    task_id='AF_DB_Q_stories_to_req_via_topics',
    python_callable=AF_DB_Q_stories_to_req_via_topics,
    dag=dag,
)

AF_request_enriching_stories_via_topics  = PythonOperator(
    task_id='AF_request_enriching_stories_via_topics',
    python_callable=AF_request_enriching_stories_via_topics,
    dag=dag,
)

AF_DB_update_stories_via_topics  = PythonOperator(
    task_id='AF_DB_update_stories_via_topics',
    python_callable=AF_DB_update_stories_via_topics,
    dag=dag,
)

AF_DB_run_author_write_process  = PythonOperator(
    task_id='AF_DB_run_author_write_process',
    python_callable=AF_DB_run_author_write_process,
    dag=dag,
)


# Set task dependencies
make_database >> make_tables
request_BBC_News >> generate_news_topten
[make_tables,generate_news_topten] >> write_extracted_story_data
write_extracted_story_data >> generate_story_List_to_be_request
generate_story_List_to_be_request >> generate_story_extract_json
generate_story_extract_json >> AF_process_updates_to_story
generate_story_extract_json >> AF_DB_insert_topics_sourced_via_storiesTT
AF_DB_insert_topics_sourced_via_storiesTT >> AF_DB_query_Topics_To_Be_Requested
AF_DB_query_Topics_To_Be_Requested >> AF_request_extract_topic_pages
AF_request_extract_topic_pages >> AF_DB_insert_stories_topics_Topics
AF_DB_insert_stories_topics_Topics >> AF_DB_Q_stories_to_req_via_topics
AF_DB_Q_stories_to_req_via_topics >> AF_request_enriching_stories_via_topics
AF_request_enriching_stories_via_topics >> AF_DB_update_stories_via_topics
AF_DB_update_stories_via_topics >> AF_DB_run_author_write_process