import threading
import time
import random

import psycopg2
from psycopg2 import errors

DB_CONFIG = {
    "dbname": "lab2_db",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432",
}

USER_ID = 1
THREADS = 10
INCREMENTS_PER_THREAD = 10000
MAX_RETRIES = 10


def init_counter():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_counter (user_id, counter, version)
                VALUES (%s, 0, 0)
                ON CONFLICT (user_id)
                DO UPDATE SET counter = EXCLUDED.counter, version = EXCLUDED.version
                """,
                (USER_ID,),
            )
        conn.commit()


def worker(thread_id: int):
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_session(isolation_level="SERIALIZABLE", autocommit=False)

    try:
        for i in range(INCREMENTS_PER_THREAD):
            retries = 0
            while True:
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT counter FROM user_counter WHERE user_id = %s FOR UPDATE",
                            (USER_ID,),
                        )
                        row = cur.fetchone()
                        if row is None:
                            raise RuntimeError("Row for user_id not found")

                        new_value = row[0] + 1

                        cur.execute(
                            "UPDATE user_counter SET counter = %s WHERE user_id = %s",
                            (new_value, USER_ID),
                        )

                    conn.commit()
                    break
                except errors.SerializationFailure:
                    conn.rollback()
                    retries += 1
                    if retries >= MAX_RETRIES:
                        print(
                            f"Thread {thread_id}: too many retries at increment {i}"
                        )
                        break
                    backoff = 0.001 * (2 ** retries)
                    time.sleep(backoff * random.random())
    finally:
        conn.close()


def main():
    init_counter()

    start = time.perf_counter()

    threads = []
    for t_id in range(THREADS):
        t = threading.Thread(target=worker, args=(t_id,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    duration_ms = (time.perf_counter() - start) * 1000

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT counter FROM user_counter WHERE user_id = %s",
                (USER_ID,),
            )
            final_value = cur.fetchone()[0]

    expected = THREADS * INCREMENTS_PER_THREAD

    print("=== Serializable update (isolation level SERIALIZABLE) ===")
    print(f"Threads: {THREADS}")
    print(f"Increments per thread: {INCREMENTS_PER_THREAD}")
    print(f"Expected value: {expected}")
    print(f"Actual value:   {final_value}")
    print(f"Execution time: {duration_ms:.0f} ms")


if __name__ == "__main__":
    main()
