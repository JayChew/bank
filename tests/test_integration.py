import requests
import time

BASE_URL = "http://app:5000"

def wait_for_app():
    for _ in range(10):
        try:
            res = requests.get(f"{BASE_URL}/health")
            if res.status_code == 200:
                return True
        except:
            time.sleep(1)
    raise Exception("App not ready")

def test_create_and_transfer():
    wait_for_app()

    # Create accounts
    r1 = requests.post(f"{BASE_URL}/accounts", json={"name": "Alice", "balance": 100})
    r2 = requests.post(f"{BASE_URL}/accounts", json={"name": "Bob", "balance": 50})
    assert r1.status_code == 201
    assert r2.status_code == 201

    # Transfer
    r3 = requests.post(f"{BASE_URL}/transfer", json={"from": "Alice", "to": "Bob", "amount": 30})
    assert r3.status_code == 200
