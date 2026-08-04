from faker import Faker
import pandas as pd
import random
from pathlib import Path

Path("data/raw").mkdir(parents=True, exist_ok=True)
Path("data/processed").mkdir(parents=True, exist_ok=True)
Path("data/analytics").mkdir(parents=True, exist_ok=True)
fake = Faker()
MERCHANTS = [
    "Amazon",
    "Flipkart",
    "Thuisbezorgd",
    "Uber",
    "Netflix",
    "Apple",
    "Steam",
    "Nike"
]

PAYMENT_METHODS = [
    "Credit Card",
    "Debit Card",
    "Net Banking"
]

CURRENCIES = [
    "INR",
    "EUR",
    "USD"
]

DEVICES = [
    "Mobile",
    "Desktop",
    "Tablet"
]
CATEGORIES = [
    "Shopping",
    "Food",
    "Transport",
    "Entertainment",
    "Healthcare",
    "Travel",
    "Bills"
]
COUNTRIES = [
    "India",
    "Netherlands",
    "Germany",
    "United Kingdom",
    "United States"
]


def generate_transaction(transaction_id):
    return {
        "transaction_id": f"TXN{transaction_id:06}",
        "customer_id": f"CUST{random.randint(1000, 9999)}",
        "timestamp": fake.date_time_this_year(),
        "merchant": random.choice(MERCHANTS),
        "amount": round(random.uniform(5, 5000), 2),
        "currency": random.choice(CURRENCIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "country": random.choice(COUNTRIES),
        "device": random.choice(DEVICES),
        "category": random.choice(CATEGORIES),
    }

transactions = []
#create synthetic data
for i in range(1, 100001):
    transactions.append(generate_transaction(i))

df = pd.DataFrame(transactions)
#inject errors
merchant_mask = df.sample(frac=0.01).index
df.loc[merchant_mask, "merchant"] = None
customer_mask = df.sample(frac=0.005).index
df.loc[customer_mask, "customer_id"] = None
#negative transactions
amount_mask = df.sample(frac=0.005).index
df.loc[amount_mask, "amount"] *= -1
#mapping errors
currency_mask = df.sample(frac=0.003).index
df.loc[currency_mask, "currency"] = "XYZ"
#invalid payment method
payment_mask = df.sample(frac=0.003).index
df.loc[payment_mask, "payment_method"] = "Crypto Potato"
#duplicate rows
duplicates = df.sample(frac=0.02)
df = pd.concat([df, duplicates], ignore_index=True)
'''
print("Rows:", len(df))
print("Missing merchants:", df["merchant"].isna().sum())
print("Missing customers:", df["customer_id"].isna().sum())
print("Negative amounts:", (df["amount"] < 0).sum())
print("Invalid currencies:", (df["currency"] == "XYZ").sum())
print("Invalid payment methods:", (df["payment_method"] == "Crypto Potato").sum())
print("Duplicate rows:", df.duplicated().sum())
'''
#save dataset

df.to_csv("data/raw/transactions.csv", index=False)

print(f"Generated {len(df)} transactions.")
