import mysql.connector
from mysql.connector import Error
from log import Bd_user_name, BD_host_name, BD_password


def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name,
        )
        print("Connection to my SQL data base")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection


def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("DataBase created successfully")
    except Exception as e:
        print(f"the error '{e} occurred'")


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f'The Error "{e} occurred"')


create_users_table = """
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT,
    NAME TEXT NOT NULL,
    price_in_sterling INT,
    info TEXT,
    detail TEXT,
    link TEXT,
    PRIMARY KEY (id)
) ENGINE = InnoDB;
"""


def get_values(connection, name, price, info, detail, link):
    create_user = """
    INSERT INTO 
        users (name, price_in_sterling, info, detail, link)
    VALUES (%s, %s, %s, %s, %s);
    """
    cursor = connection.cursor()
    cursor.execute(create_user, (name, price, info, detail, link))
    connection.commit()


#connect = create_connection(BD_host_name, Bd_user_name, BD_password, "rent_price_uk")
#create_database_query = "CREATE DATABASE IF NOT EXISTS rent_price_uk"
#create_database(connect, create_database_query)
#execute_query(connect, create_users_table)