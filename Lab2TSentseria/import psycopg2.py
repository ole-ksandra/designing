import psycopg2

def main():
    connection = psycopg2.connect(
        dbname="lab2_db",
        user="postgres",
        password="123456",
        host="localhost",
        port="5432"
    )

    cursor = connection.cursor()
    cursor.execute("SELECT user_id, counter, version FROM user_counter;")
    row = cursor.fetchone()
    print("Row from user_counter:", row)

    cursor.close()
    connection.close()

if __name__ == "__main__":
    main()