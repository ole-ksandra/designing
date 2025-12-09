import threading
import time
import psycopg2
from psycopg2.errors import SerializationFailure

DB_CONFIG = {
    "dbname": "lab2_db",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432",
}

THREADS = 10
INCREMENTS_PER_THREAD = 10000
MAX_RETRIES = 10


def reset_counter():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("UPDATE user_counter SET counter = 0, version = 0 WHERE user_id = 1;")
    cur.close()
    conn.close()


def worker_serializable(thread_id: int):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_session(isolation_level="SERIALIZABLE", autocommit=False)
    cur = conn.cursor()

    for i in range(INCREMENTS_PER_THREAD):
        for attempt in range(MAX_RETRIES):
            try:
                cur.execute("SELECT counter FROM user_counter WHERE user_id = 1;")
                row = cur.fetchone()
                counter = row[0] + 1

                cur.execute(
                    "UPDATE user_counter SET counter = %s WHERE user_id = %s;",
                    (counter, 1),
                )

                conn.commit()
                break
            except SerializationFailure:
                conn.rollback()
                if attempt == MAX_RETRIES - 1:
                    print(f"Thread {thread_id}: giving up after {MAX_RETRIES} retries")
            except Exception as e:
                conn.rollback()
                print(f"Thread {thread_id}: unexpected error: {e}")
                conn.close()
                return

    cur.close()
    conn.close()


def read_final_counter() -> int:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT counter FROM user_counter WHERE user_id = 1;")
    value = cur.fetchone()[0]
    cur.close()
    conn.close()
    return value


def main():
    reset_counter()

    start = time.perf_counter()

    threads = []
    for t_id in range(THREADS):
        t = threading.Thread(target=worker_serializable, args=(t_id,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    end = time.perf_counter()
    final_value = read_final_counter()

    print("=== Serializable update with retries ===")
    print(f"Threads: {THREADS}")
    print(f"Increments per thread: {INCREMENTS_PER_THREAD}")
    print(f"Expected value: {THREADS * INCREMENTS_PER_THREAD}")
    print(f"Actual value:   {final_value}")
    print(f"Execution time: {end - start:.3f} seconds")


if __name__ == "__main__":
    main()
