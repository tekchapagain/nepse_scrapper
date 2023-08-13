import requests
import csv
import json

def transform_csv_row(row):
    return {
        "company": {
            "code": row["SYMBOL"],
            "name": row["SECURITY_NAME"]
        },
        "price": {
            "open": float(row["OPEN_PRICE"]),
            "max": float(row["HIGH_PRICE"]),
            "min": float(row["LOW_PRICE"]),
            "close": float(row["CLOSE_PRICE"]),
            "prevClose": float(row["PREVIOUS_DAY_CLOSE_PRICE"]),
            "diff": float(row["CLOSE_PRICE"]) - float(row["PREVIOUS_DAY_CLOSE_PRICE"])
        },
        "numTrans": float(row["TOTAL_TRADES"]),
        "tradedShares": float(row["TOTAL_TRADED_QUANTITY"]),
        "amount": float(row["TOTAL_TRADED_VALUE"])
    }

def fetch_data(date):
    if not date:
        raise ValueError("Date parameter is required")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'
    }
    
    response = requests.get(f"https://www.nepalstock.com.np/api/nots/market/export/todays-price/{date}", headers=headers)
    response.raise_for_status()
    
    csv_data = response.text
    csv_rows = list(csv.DictReader(csv_data.splitlines()))
    
    return csv_rows

def scrape_companies_data(data):
    companies = {}
    for d in data:
        companies[d["SYMBOL"]] = {
            "id": d["SECURITY_ID"],
            "name": d["SECURITY_NAME"]
        }
    with open("./data/companies.json", "w") as file:
        json.dump(companies, file)

def scrape_market_data(csv_rows, date):
    meta = {
        "totalAmt": 0.00,
        "totalQty": 0,
        "totalTrans": 0
    }
    
    stocks_data = []
    
    for row in csv_rows:
        meta["totalAmt"] += float(row["TOTAL_TRADED_VALUE"])
        meta["totalQty"] += float(row["TOTAL_TRADED_QUANTITY"])
        meta["totalTrans"] += float(row["TOTAL_TRADES"])
        stocks_data.append(transform_csv_row(row))
    
    merged_data = {
        "metadata": meta,
        "data": stocks_data
    }
    
    with open(f"./data/date/{date}.json", "w") as file:
        json.dump(merged_data, file)
    with open("./data/date/today.json", "w") as file:
        json.dump(merged_data, file)
    with open("./data/date/latest.json", "w") as file:
        json.dump(merged_data, file)

def group_market_data_by_company(csv_rows, date):
    for row in csv_rows:
        stock_data = transform_csv_row(row)
        
        if stock_data and stock_data["company"] and stock_data["company"]["code"]:
            existing_data = {}
            company_code = stock_data["company"]["code"]
            try:
                with open(f"./data/company/{company_code.replace('/',company_code)}.json", "r") as file:
                    existing_data = json.load(file)
            except Exception as e:
                print(e)
            
            del stock_data["company"]
            existing_data[date] = stock_data
            
            with open(f"./data/company/{company_code.replace('/', company_code)}.json", "w") as file:
                json.dump(existing_data, file)

# # Usage example
# date = "2023-08-12"  # Replace with the desired date
# csv_rows = fetch_data(date)

# scrape_companies_data(csv_rows)
# scrape_market_data(csv_rows, date)
# group_market_data_by_company(csv_rows, date)
