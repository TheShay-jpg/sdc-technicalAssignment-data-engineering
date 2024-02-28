import requests
import urllib3
import mysql.connector
from mysql.connector import Error

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_news(api_key, country='eu', from_date='2024-02-25', to_date='2024-02-26'):
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

# Example usage:
api_key = '7538bd8775004d5bb5166f5574cd9abd'
articles = fetch_news(api_key)

# MySQL connection configuration
host_name = "localhost"
user_name = "root"
user_password = "12345"
db_name = "testDB"

connection = create_connection(host_name, user_name, user_password, db_name)

# Drop table if already created
drop_table_query = """
DROP TABLE IF EXISTS news_articles
"""
execute_query(connection, drop_table_query)

# Create table to store articles if not exists
create_table_query = """
CREATE TABLE IF NOT EXISTS news_articles (
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
    INSERT INTO news_articles (source, author, title, description, url, published_at)
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    values = (source, author, title, description, url, published_at)
    execute_query(connection, insert_query, values)

# Select articles from the table
select_query = "SELECT * FROM news_articles"
articles = execute_query(connection, select_query)

# Print the articles
if articles:
    for article in articles:
        print(article)

# Close the connection
if connection:
    connection.close()