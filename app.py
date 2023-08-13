from flask import Flask, request, jsonify
from datetime import datetime
from scrapper import fetch_data, scrape_companies_data, scrape_market_data, group_market_data_by_company
import json

app = Flask(__name__)

@app.route("/scrape", methods=["GET"])
def scrape_data():
    try:   
        date = datetime.now().strftime("%Y-%m-%d")
        csv_rows = fetch_data(date)
        scrape_companies_data(csv_rows)
        scrape_market_data(csv_rows, date)
        data = group_market_data_by_company(csv_rows, date)
        return jsonify({"data": data}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route("/stock/<company_code>", methods=["GET"])
def get_stock_data(company_code):
    try:
        code = company_code
        with open(f"data/company/{code}.json", "r") as file:
            data = json.load(file)
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route("/todays_price", methods=["GET"])
def todays_price():
    try:
        with open(f"data/date/today.json", "r") as file:
            data = json.load(file)
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/history/<date>", methods=["GET"])
def history_data(date):
    try:
        with open(f"data/date/{date}.json", "r") as file:
            data = json.load(file)
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": "History data not availble"}), 500
    
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
