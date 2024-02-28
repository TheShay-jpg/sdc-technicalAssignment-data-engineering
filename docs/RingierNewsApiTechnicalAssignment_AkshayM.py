import requests
import urllib3
import mysql.connector
from mysql.connector import Error
import textblob
from textblob import TextBlob


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Example usage:
api_key = '7538bd8775004d5bb5166f5574cd9abd'

# MySQL connection configuration
host_name = "localhost"
user_name = "root"
user_password = "12345"
db_name = "testDB"


def fetch_news(api_key, country='eu', from_date='2024-02-10', to_date='2024-02-27'):
    url = f'https://newsapi.org/v2/everything?q=Europe&from={from_date}&to={to_date}&apiKey={api_key}'
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        if data.get('status') == 'ok':
            return data['articles']
        else:
            print(f'Failed to fetch news articles: {data.get("message")}')
            return []
    except requests.exceptions.RequestException as e:
        print(f'Error fetching news articles: {e}')
        return []

def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def execute_query(connection, query, values=None):
    cursor = connection.cursor()
    try:
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")


articles = fetch_news(api_key)



connection = create_connection(host_name, user_name, user_password, db_name)

# Drop table if already created
drop_table_query = """
DROP TABLE IF EXISTS news_articles
"""
execute_query(connection, drop_table_query)

drop_table_query = """
DROP TABLE IF EXISTS raw_data
"""
execute_query(connection, drop_table_query)

# Create table to store articles if not exists
create_table_query = """
CREATE TABLE IF NOT EXISTS raw_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source VARCHAR(255),
    author VARCHAR(255),
    title TEXT,
    description TEXT,
    url VARCHAR(255),
    published_at VARCHAR(255)
)
"""
execute_query(connection, create_table_query)

# Insert articles into the table
for article in articles:
    source = article['source']['name']
    author = article['author']
    title = article['title']
    description = article['description']
    url = article['url']
    published_at = article['publishedAt']

    insert_query = """
    INSERT INTO raw_data (source, author, title, description, url, published_at)
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    values = (source, author, title, description, url, published_at)
    execute_query(connection, insert_query, values)


# Print the articles
if articles:
    for article in articles:
        print(article)




def process_data(connection):
    # Drop table if already created
    drop_table_query = """
    DROP TABLE IF EXISTS processed_data
    """
    execute_query(connection, drop_table_query)

    create_table_query = """
    CREATE TABLE IF NOT EXISTS processed_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source VARCHAR(255),
        author VARCHAR(255),
        title TEXT,
        description TEXT,
        url VARCHAR(255),
        published_at VARCHAR(255)
    )
    """
    execute_query(connection, create_table_query)

    # Insert valid records into processed_data table
    insert_processed_query = """
        INSERT INTO processed_data (source, author, title, description, url, published_at)
        SELECT source, author, title, description, url, published_at
        FROM raw_data
        WHERE description != '' AND description != '[Removed]'
        """
    execute_query(connection, insert_processed_query)


process_data(connection)

def invalid_data(connection):
    # Drop table if already created
    drop_table_query = """
        DROP TABLE IF EXISTS invalid_data
        """
    execute_query(connection, drop_table_query)

    create_table_query = """
    CREATE TABLE IF NOT EXISTS invalid_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source VARCHAR(255),
        author VARCHAR(255),
        title TEXT,
        description TEXT,
        url VARCHAR(255),
        published_at VARCHAR(255)
    )
    """
    execute_query(connection, create_table_query)



    # Insert valid records into processed_data table
    insert_invalid_query = """
        INSERT INTO invalid_data (source, author, title, description, url, published_at)
        SELECT source, author, title, description, url, published_at
        FROM raw_data
        WHERE id NOT IN (SELECT id FROM processed_data)
        """
    execute_query(connection, insert_invalid_query)


invalid_data(connection)

def sentiment_analysis(connection):
    # Create analyze_data table with the same structure as processed_data
    create_analyze_table_query = """
    CREATE TABLE IF NOT EXISTS analyze_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source VARCHAR(255),
        author VARCHAR(255),
        title TEXT,
        description TEXT,
        url VARCHAR(255),
        published_at VARCHAR(255),
        sentiment_value FLOAT,
        sentiment_text VARCHAR(20)
    )
    """
    execute_query(connection, create_analyze_table_query)

    # Retrieve data from processed_data table
    select_processed_query = """
    SELECT id, source, author, title, description, url, published_at
    FROM processed_data
    """
    cursor = connection.cursor(dictionary=True)
    cursor.execute(select_processed_query)
    processed_data = cursor.fetchall()

    # Perform sentiment analysis and insert data into analyze_data table
    insert_analyze_query = """
    INSERT INTO analyze_data (source, author, title, description, url, published_at, sentiment_value, sentiment_text)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    for row in processed_data:
        description = row['description']
        sentiment_score = TextBlob(description).sentiment.polarity * 100
        if sentiment_score > 0:
            sentiment_text = 'positive'
        elif sentiment_score < 0:
            sentiment_text = 'negative'
        else:
            sentiment_text = 'neutral'
        values = (row['source'], row['author'], row['title'], row['description'], row['url'], row['published_at'], sentiment_score, sentiment_text)
        execute_query(connection, insert_analyze_query, values)



sentiment_analysis(connection)

# Close the connection
if connection:
    connection.close()