from flask import Flask, request, jsonify
import psycopg2
import os

app = Flask(__name__)

def get_db_connection():
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
    )
    return conn

@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/accounts", methods=["POST"])
def create_account():
    data = request.json
    name = data["name"]
    balance = data.get("balance", 0)

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO accounts (name, balance) VALUES (%s, %s)", (name, balance))
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"message": f"Account {name} created"}), 201


@app.route("/transfer", methods=["POST"])
def transfer():
    data = request.json
    sender = data["from"]
    receiver = data["to"]
    amount = data["amount"]

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("UPDATE accounts SET balance = balance - %s WHERE name = %s", (amount, sender))
    cur.execute("UPDATE accounts SET balance = balance + %s WHERE name = %s", (amount, receiver))
    conn.commit()

    cur.close()
    conn.close()

    return jsonify({"message": f"Transferred {amount} from {sender} to {receiver}"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
