from flask import Flask, jsonify
from flask_cors import CORS
import psycopg2
import pandas as pd

DB_CONFIG = {
    'database': 'primary_postgres',
    'user': 'postgres',
    'password': 'diansdomashna',
    'host': 'localhost',
    'port': '5440'
}

app = Flask(__name__)
CORS(app)  

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

@app.route('/api/analyzed-data/<symbol>', methods=['GET'])
def get_analyzed_data(symbol):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT "analysis_date", "symbol", "latest_price", "SMA_5", "SMA_10", "predicted_next_close", "buy_sell_hold"
            FROM data_analysis_results
            WHERE "symbol" = %s
            ORDER BY "analysis_date" DESC
            LIMIT 100;
        """, (symbol,))

        rows = cur.fetchall()
        conn.close()

        analyzed_data = [
            {
                'date': row[0],
                'symbol': row[1],
                'latest_price': row[2],
                'SMA_5': row[3],
                'SMA_10': row[4],
                'prediction': row[5],
                'recommendation': row[6],
                'trend': row[3]  # SMA_5 as the trend
            }
            for row in rows
        ]

        return jsonify(analyzed_data)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, port=5001)
