import requests
import urllib3
import mysql.connector
from mysql.connector import Error
import pandas as pd
import mysql.connector
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from sqlalchemy import create_engine
from nltk.corpus import stopwords
import nltk
nltk.download('stopwords')

# Input Values
api_key = '7538bd8775004d5bb5166f5574cd9abd'

# MySQL Connection Config
host_name = "localhost"
user_name = "root"
user_password = "12345"
db_name = "testDB"

#InsecureRequest preventing pull of data in fetch_news()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
def fetch_news(api_key, country='eu', from_date='2024-02-22', to_date='2024-02-29'):
    url = f'https://newsapi.org/v2/everything?q=Europe&from={from_date}&to={to_date}&apiKey={api_key}'
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        if data.get('status') == 'ok': #If WebApp responsive proceed
            return data['articles']
        else:
            print(f'Failed to fetch news articles: {data.get("message")}')
            return []
    except requests.exceptions.RequestException as e: #Catch RequestException and return error
        print(f'Error fetching news articles: {e}')
        return []

#define function used to create a connection to instantiated MySQl DB
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

#Define function to execute queries with optional "Value" parameter
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

#Perform NewsAPI Extraction
articles = fetch_news(api_key)


#Instantiate a Connection
connection = create_connection(host_name, user_name, user_password, db_name)

# Drop table raw_data if already created
drop_table_query = """
DROP TABLE IF EXISTS raw_data
"""
execute_query(connection, drop_table_query)


# Create raw_data table to store articles if not exists
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
#Execute Query Call to Create raw_data table
execute_query(connection, create_table_query)

# Insert articles into the raw table
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


#Define function to create processed_data table which
#outputs raw_data that has undergone remedial transformation / filtering to cleanse data

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

#Define function to catch all invalid data used for reporting purposes
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
        WHERE title NOT IN (SELECT title FROM processed_data)
        """
    execute_query(connection, insert_invalid_query)


invalid_data(connection)

# Create an SQLAlchemy engine using the MySQL connector connection
engine = create_engine('mysql+mysqlconnector://root:12345@localhost/testDB')

# Fetch the processed_data from the processed_data table
query = "SELECT * FROM processed_data"
processed_data = pd.read_sql(query, engine)


# Sentiment analysis
analyzer = SentimentIntensityAnalyzer()
processed_data['sentiment_score'] = processed_data['description'].apply(lambda x: analyzer.polarity_scores(x)['compound'])
processed_data['sentiment_text'] = processed_data['sentiment_score'].apply(lambda x: 'positive' if x > 0 else ('negative' if x < 0 else 'neutral'))

# Topic modeling
# Tokenize the text and remove stopwords
stop_words = list(stopwords.words('english'))
vectorizer = CountVectorizer(stop_words=stop_words)
X = vectorizer.fit_transform(processed_data['description'])

# Train the LDA model
lda_model = LatentDirichletAllocation(n_components=8, random_state=42)
lda_model.fit(X)

# Assign topics to each document
topics_covered = lda_model.transform(X).argmax(axis=1)
processed_data['topics_covered'] = topics_covered

# Drop table if already created
drop_table_query = """
    DROP TABLE IF EXISTS analyze_data
    """
execute_query(connection, drop_table_query)

processed_data.to_sql('analyze_data', engine, if_exists='replace', index=False)

engine.dispose()
# Close the connection
if connection:
    connection.close()

