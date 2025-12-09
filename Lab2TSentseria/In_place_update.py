import psycopg2
import threading
import time

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
    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE user_counter SET counter = 0, version = 0 WHERE user_id = 1;"
            )


def worker(thread_id: int):
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = False
    try:
        for _ in range(INCREMENTS_PER_THREAD):
            with conn.cursor() as cursor:
                cursor.execute("BEGIN;")
                cursor.execute(
                    "UPDATE user_counter "
                    "SET counter = counter + 1 "
                    "WHERE user_id = 1;"
                )
                conn.commit()
        print(f"Thread {thread_id}: finished")
    finally:
        conn.close()


def get_counter() -> int:
    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT counter FROM user_counter WHERE user_id = 1;"
            )
            row = cursor.fetchone()
            return row[0]


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

    print("=== In-place update ===")
    print(f"Threads: {THREADS}")
    print(f"Increments per thread: {INCREMENTS_PER_THREAD}")
    print(f"Expected value: {expected}")
    print(f"Actual value:   {actual}")
    print(f"Execution time: {end - start:.3f} seconds")


if __name__ == "__main__":
    main()
