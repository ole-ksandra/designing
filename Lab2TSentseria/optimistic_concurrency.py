import threading
import time
import psycopg2

DB_PARAMS = {
    "dbname": "lab2_db",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432",
}

THREADS = 10
INCREMENTS_PER_THREAD = 10000


def reset_counter():
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    try:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE user_counter SET counter = 0, version = 0 WHERE user_id = 1;")
    finally:
        conn.close()


def get_counter():
    conn = psycopg2.connect(**DB_PARAMS)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1;")
            row = cursor.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def worker(thread_id: int):
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = False
    try:
        for _ in range(INCREMENTS_PER_THREAD):
            success = False
            while not success:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "SELECT counter, version "
                            "FROM user_counter "
                            "WHERE user_id = 1;"
                        )
                        row = cursor.fetchone()
                        if row is None:
                            conn.rollback()
                            return
                        current = row[0]
                        version = row[1]
                        new_counter = current + 1
                        new_version = version + 1
                        cursor.execute(
                            "UPDATE user_counter "
                            "SET counter = %s, version = %s "
                            "WHERE user_id = %s AND version = %s;",
                            (new_counter, new_version, 1, version),
                        )
                        if cursor.rowcount == 1:
                            conn.commit()
                            success = True
                        else:
                            conn.rollback()
                except psycopg2.Error:
                    conn.rollback()
                if not success:
                    time.sleep(0.001)
        print(f"Thread {thread_id}: finished")
    finally:
        conn.close()


def main():
    reset_counter()

    threads = []
    start = time.time()
    for i in range(THREADS):
        t = threading.Thread(target=worker, args=(i,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    end = time.time()

    expected = THREADS * INCREMENTS_PER_THREAD
    actual = get_counter()

    print("=== Optimistic concurrency control ===")
    print(f"Threads: {THREADS}")
    print(f"Increments per thread: {INCREMENTS_PER_THREAD}")
    print(f"Expected value: {expected}")
    print(f"Actual value:   {actual}")
    print(f"Execution time: {end - start:.3f} seconds")


if __name__ == "__main__":
    main()
