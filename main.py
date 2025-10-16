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
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS accounts (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            balance INTEGER
        )
    ''')
    conn.commit()

    cursor.execute("DELETE FROM accounts;")
    cursor.executemany(
        "INSERT INTO accounts (name, balance) VALUES (%s, %s)",
        [('Alice', 100), ('Bob', 100)]
    )
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Database initialized.\n")

# === 单笔事务函数 ===
def transfer(sender, receiver, amount):
    conn = connect()
    cursor = conn.cursor()
    try:
        conn.autocommit = False
        cursor.execute("SELECT balance FROM accounts WHERE name=%s FOR UPDATE;", (sender,))
        balance = cursor.fetchone()[0]
        if balance < amount:
            raise ValueError("Insufficient funds")
        cursor.execute("UPDATE accounts SET balance = balance - %s WHERE name = %s;", (amount, sender))
        cursor.execute("UPDATE accounts SET balance = balance + %s WHERE name = %s;", (amount, receiver))
        conn.commit()
        print(f"✅ Transferred {amount} from {sender} to {receiver}")
    except Exception as e:
        conn.rollback()
        print(f"❌ Transaction failed: {e}")
    finally:
        cursor.close()
        conn.close()

# === 并发事务测试 ===
def thread_a():
    conn = connect()
    cur = conn.cursor()
    conn.autocommit = False

    print("\n[Thread A] BEGIN TRANSACTION")
    cur.execute("BEGIN;")
    cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")

    cur.execute("SELECT balance FROM accounts WHERE name='Alice' FOR UPDATE;")
    balance = cur.fetchone()[0]
    print(f"[Thread A] Read balance: {balance}")

    print("[Thread A] Simulating processing (sleep 5s)...")
    time.sleep(5)

    cur.execute("UPDATE accounts SET balance = balance - 50 WHERE name='Alice';")
    conn.commit()
    print("[Thread A] Transaction COMMITTED (balance -50)")
    cur.close()
    conn.close()

def thread_b():
    time.sleep(2)
    conn = connect()
    cur = conn.cursor()
    conn.autocommit = False

    print("\n[Thread B] BEGIN TRANSACTION")
    cur.execute("BEGIN;")
    cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")

    cur.execute("SELECT balance FROM accounts WHERE name='Alice';")
    balance1 = cur.fetchone()[0]
    print(f"[Thread B] First read: {balance1}")

    print("[Thread B] Waiting (sleep 5s)...")
    time.sleep(5)

    cur.execute("SELECT balance FROM accounts WHERE name='Alice';")
    balance2 = cur.fetchone()[0]
    print(f"[Thread B] Second read: {balance2}")

    conn.commit()
    cur.close()
    conn.close()

# === 主执行逻辑 ===
if __name__ == "__main__":
    init_db()

    print("=== 单笔事务测试 ===")
    transfer('Alice', 'Bob', 50)
    transfer('Bob', 'Alice', 200)

    conn = connect()
    cursor = conn.cursor()
    cursor.execute("SELECT name, balance FROM accounts;")
    print("\nFinal Balances:")
    for row in cursor.fetchall():
        print(row)
    cursor.close()
    conn.close()

    print("\n=== 并发事务测试（Isolation Level: READ COMMITTED） ===")
    # 重设初始余额
    conn = connect()
    cur = conn.cursor()
    cur.execute("UPDATE accounts SET balance = 100 WHERE name='Alice';")
    conn.commit()
    cur.close()
    conn.close()

    t1 = threading.Thread(target=thread_a)
    t2 = threading.Thread(target=thread_b)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # 显示并发事务后的最终结果
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT name, balance FROM accounts;")
    print("\nFinal Balances After Concurrency Test:")
    for row in cur.fetchall():
        print(row)
    cur.close()
    conn.close()
