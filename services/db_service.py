import mysql.connector
from mysql.connector import Error


class DatabaseService:
    def __init__(self, host, user, password, port=3306):
        self.host = host
        self.user = user
        self.password = password
        self.port = port

    def fetch_data(self, database, query):
        try:
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port,
                database=database
            )
            if connection.is_connected():
                cursor = connection.cursor(dictionary=True)
                cursor.execute(query)
                result = cursor.fetchall()
                return result
        except Error as e:
            print(f"Error while connecting to MySQL: {e}")
            return None
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()
