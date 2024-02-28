import mysql.connector

host = 'localhost'  # Replace with the IP address of your MySQL container
port = 3306         # Replace with the port number of your MySQL container
user = 'root'       # MySQL username
password = '12345'  # MySQL password
database = 'NewsAPIDB'  # Name of the database you want to connect to

# Establish a connection
try:
    connection = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    print("Connected to MySQL database!")
except mysql.connector.Error as err:
    print(f"Error: {err}")