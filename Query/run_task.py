config = {
    'user': 'root',
    'password': 'example',  # Use your root password here
    'host': 'localhost',
    'port': 3307,
    'database': 'DEV_TheBBCdata'
}


from methods import drop_database


drop_database(config,"DEV_TheBBCdata")