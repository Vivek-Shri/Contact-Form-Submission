import requests
url = "http://64.227.188.12:8001/api/campaigns/test_campaign/contacts/bulk"
payload = {
    "contacts": [
        {"companyName": "Test Co", "contactUrl": "http://test.com"}
    ]
}
try:
    res = requests.post(url, json=payload)
    print(f"Status: {res.status_code}")
    print(res.text)
except Exception as e:
    print(e)
