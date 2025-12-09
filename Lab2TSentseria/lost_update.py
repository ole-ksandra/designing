import threading
import time
import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "lab2_db",
    "user": "postgres",
    "password": "123456",
}

NUM_THREADS = 10
INCREMENTS_PER_THREAD = 10000
USER_ID = 1


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def reset_counter():
    conn = get_connection()
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("UPDATE user_counter SET counter = 0, version = 0 WHERE user_id = %s", (USER_ID,))
        cur.close()
    finally:
        conn.close()


def lost_update_worker():
    conn = get_connection()
    try:
        conn.autocommit = False
        cur = conn.cursor()
        for _ in range(INCREMENTS_PER_THREAD):
            cur.execute("SELECT counter FROM user_counter WHERE user_id = %s", (USER_ID,))
            row = cur.fetchone()
            current = row[0]
            new_value = current + 1
            cur.execute(
                "UPDATE user_counter SET counter = %s WHERE user_id = %s",
                (new_value, USER_ID),
            )
            conn.commit()
        cur.close()
    finally:
        conn.close()


def run_lost_update():
    reset_counter()

    start = time.perf_counter()

    threads = []
    for i in range(NUM_THREADS):
        t = threading.Thread(target=lost_update_worker, name=f"worker-{i}")
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    elapsed = time.perf_counter() - start

    conn = get_connection()
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT counter FROM user_counter WHERE user_id = %s", (USER_ID,))
        final_value = cur.fetchone()[0]
        cur.close()
    finally:
        conn.close()

    expected = NUM_THREADS * INCREMENTS_PER_THREAD

    print("=== Lost-update scenario (READ COMMITTED) ===")
    print(f"Threads: {NUM_THREADS}")
    print(f"Increments per thread: {INCREMENTS_PER_THREAD}")
    print(f"Expected value: {expected}")
    print(f"Actual value:   {final_value}")
    print(f"Execution time: {elapsed:.3f} s")


if __name__ == "__main__":
    run_lost_update()
