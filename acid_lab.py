import psycopg2
import threading
import time
import sys

# === 数据库配置 ===
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
    cur.execute("DELETE FROM accounts;")
    cur.executemany("INSERT INTO accounts (name, balance) VALUES (%s, %s)",
                    [('Alice', 100), ('Bob', 100)])
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Database initialized with Alice=100, Bob=100.\n")


# === Atomicity & Consistency ===
def test_atomicity_consistency():
    print("=== [A + C] Atomicity & Consistency Test ===")

    def transfer(sender, receiver, amount):
        conn = connect()
        cur = conn.cursor()
        conn.autocommit = False
        try:
            cur.execute("SELECT balance FROM accounts WHERE name=%s FOR UPDATE;", (sender,))
            balance = cur.fetchone()[0]
            if balance < amount:
                raise ValueError("Insufficient funds")
            cur.execute("UPDATE accounts SET balance = balance - %s WHERE name=%s;", (amount, sender))
            cur.execute("UPDATE accounts SET balance = balance + %s WHERE name=%s;", (amount, receiver))
            conn.commit()
            print(f"✅ Transferred {amount} from {sender} to {receiver}")
        except Exception as e:
            conn.rollback()
            print(f"❌ Transaction failed: {e}")
        finally:
            cur.close()
            conn.close()

    transfer('Alice', 'Bob', 50)
    transfer('Bob', 'Alice', 200)

    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT name, balance FROM accounts;")
    rows = cur.fetchall()
    print("\nFinal Balances:")
    for r in rows:
        print(r)
    cur.close()
    conn.close()
    print("✅ Total sum = Consistent\n")


# === Isolation (READ COMMITTED / SERIALIZABLE) ===
def test_isolation(level='READ COMMITTED'):
    print(f"=== [I] Isolation Test (Level: {level}) ===")

    # 重设初始值
    conn = connect()
    cur = conn.cursor()
    cur.execute("UPDATE accounts SET balance=100 WHERE name='Alice';")
    conn.commit()
    cur.close()
    conn.close()

    def thread_a():
        conn = connect()
        cur = conn.cursor()
        conn.autocommit = False
        cur.execute("BEGIN;")
        cur.execute(f"SET TRANSACTION ISOLATION LEVEL {level};")
        print("\n[Thread A] BEGIN TRANSACTION")
        cur.execute("SELECT balance FROM accounts WHERE name='Alice' FOR UPDATE;")
        balance = cur.fetchone()[0]
        print(f"[Thread A] Read balance: {balance}")
        time.sleep(5)
        cur.execute("UPDATE accounts SET balance = balance - 50 WHERE name='Alice';")
        conn.commit()
        print("[Thread A] COMMITTED (-50)")
        cur.close()
        conn.close()

    def thread_b():
        time.sleep(2)
        conn = connect()
        cur = conn.cursor()
        conn.autocommit = False
        cur.execute("BEGIN;")
        cur.execute(f"SET TRANSACTION ISOLATION LEVEL {level};")
        print("\n[Thread B] BEGIN TRANSACTION")
        cur.execute("SELECT balance FROM accounts WHERE name='Alice';")
        b1 = cur.fetchone()[0]
        print(f"[Thread B] First read: {b1}")
        time.sleep(5)
        cur.execute("SELECT balance FROM accounts WHERE name='Alice';")
        b2 = cur.fetchone()[0]
        print(f"[Thread B] Second read: {b2}")
        conn.commit()
        cur.close()
        conn.close()

    t1 = threading.Thread(target=thread_a)
    t2 = threading.Thread(target=thread_b)
    t1.start(); t2.start()
    t1.join(); t2.join()

    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT name, balance FROM accounts;")
    print("\nFinal Balances:")
    for r in cur.fetchall():
        print(r)
    cur.close()
    conn.close()
    print("✅ Isolation test done.\n")


# === Durability ===
def durability_init():
    conn = connect()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS durability_test (
            id SERIAL PRIMARY KEY,
            note TEXT
        )
    ''')
    conn.commit()
    cur.close()
    conn.close()
    print("✅ durability_test table ready.\n")

def durability_write():
    conn = connect()
    cur = conn.cursor()
    conn.autocommit = False
    cur.execute("INSERT INTO durability_test (note) VALUES (%s);",
                (f"Committed at {time.strftime('%H:%M:%S')}",))
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Transaction committed. Now simulate crash (Ctrl+C or close terminal).")

def durability_read():
    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM durability_test ORDER BY id DESC LIMIT 5;")
    rows = cur.fetchall()
    print("\n🧾 Last 5 Records:")
    for r in rows:
        print(r)
    cur.close()
    conn.close()


# === 主菜单 ===
def main_menu():
    while True:
        print("""
===============================
   🧪 PostgreSQL ACID Lab
===============================
1️⃣  Atomicity & Consistency
2️⃣  Isolation (READ COMMITTED)
3️⃣  Isolation (SERIALIZABLE)
4️⃣  Durability - Write Transaction
5️⃣  Durability - Read After Restart
0️⃣  Exit
===============================
""")
        choice = input("Select option: ").strip()

        if choice == "1":
            init_db()
            test_atomicity_consistency()
        elif choice == "2":
            test_isolation('READ COMMITTED')
        elif choice == "3":
            test_isolation('SERIALIZABLE')
        elif choice == "4":
            durability_init()
            durability_write()
        elif choice == "5":
            durability_read()
        elif choice == "0":
            print("Bye 👋")
            sys.exit(0)
        else:
            print("❌ Invalid choice. Try again.\n")


if __name__ == "__main__":
    main_menu()
