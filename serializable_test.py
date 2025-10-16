import psycopg2
import threading
import time

# === 数据库连接配置 ===
DB_CONFIG = {
    'dbname': 'bank',
    'user': 'postgres',
    'password': 'root',
    'host': 'localhost',
    'port': '5432'
}

def connect():
    return psycopg2.connect(**DB_CONFIG)

# === 初始化数据库 ===
def init_db():
    conn = connect()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS accounts (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            balance INTEGER
        )
    ''')
    conn.commit()

    cur.execute("DELETE FROM accounts;")
    cur.executemany(
        "INSERT INTO accounts (name, balance) VALUES (%s, %s)",
        [('Alice', 100), ('Bob', 100)]
    )
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Database initialized.\n")

# === 并发事务（SERIALIZABLE 实验） ===
def thread_a():
    conn = connect()
    cur = conn.cursor()
    conn.autocommit = False
    print("\n[Thread A] BEGIN TRANSACTION (SERIALIZABLE)")
    cur.execute("BEGIN;")
    cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")

    cur.execute("SELECT balance FROM accounts WHERE name='Alice';")
    balance = cur.fetchone()[0]
    print(f"[Thread A] Read balance: {balance}")
    time.sleep(3)

    cur.execute("UPDATE accounts SET balance = balance - 30 WHERE name='Alice';")
    try:
        conn.commit()
        print("[Thread A] ✅ COMMITTED (balance -30)")
    except Exception as e:
        print(f"[Thread A] ❌ ROLLBACK: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def thread_b():
    time.sleep(1)  # 稍微延迟以模拟竞争
    conn = connect()
    cur = conn.cursor()
    conn.autocommit = False
    print("\n[Thread B] BEGIN TRANSACTION (SERIALIZABLE)")
    cur.execute("BEGIN;")
    cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")

    cur.execute("SELECT balance FROM accounts WHERE name='Alice';")
    balance = cur.fetchone()[0]
    print(f"[Thread B] Read balance: {balance}")
    time.sleep(3)

    cur.execute("UPDATE accounts SET balance = balance - 20 WHERE name='Alice';")
    try:
        conn.commit()
        print("[Thread B] ✅ COMMITTED (balance -20)")
    except Exception as e:
        print(f"[Thread B] ❌ ROLLBACK: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

# === 主执行逻辑 ===
if __name__ == "__main__":
    init_db()

    print("=== 并发事务测试（Isolation Level: SERIALIZABLE） ===")
    t1 = threading.Thread(target=thread_a)
    t2 = threading.Thread(target=thread_b)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # 查看最终余额
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT name, balance FROM accounts;")
    print("\nFinal Balances After SERIALIZABLE Test:")
    for row in cur.fetchall():
        print(row)
    cur.close()
    conn.close()
