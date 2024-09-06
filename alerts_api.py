from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import psycopg2.pool
import logging
import config

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)

# Set up the connection pool
connection_pool = psycopg2.pool.SimpleConnectionPool(1, 10, user=config.DB_USER,
                                                     password=config.DB_PASS,
                                                     host=config.DB_HOST,
                                                     database=config.DB_NAME)

@app.route('/consolidated_holdings', methods=['GET'])
def get_consolidated_holdings():
    conn = connection_pool.getconn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM consolidated_holdings")
        holdings = cur.fetchall()
        return jsonify([dict(holding) for holding in holdings])
    finally:
        cur.close()
        connection_pool.putconn(conn)

@app.route('/alerts', methods=['GET'])
def get_alerts():
    conn = connection_pool.getconn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM alerts order by symbol, alert_type, alert_direction, alert_level, create_date")
        alerts = cur.fetchall()
        return jsonify([dict(alert) for alert in alerts])
    finally:
        cur.close()
        connection_pool.putconn(conn)

@app.route('/alerts', methods=['POST'])
def create_alert():
    try:
        data = request.get_json()
    except:
        return jsonify({"error": "Invalid JSON"}), 400

    required_fields = ['symbol', 'alert_type', 'alert_direction', 'alert_level']
    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing required fields", "data":data}), 400

    alert_levels = data['alert_level'].split(',')

    conn = connection_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN;")
            for level in alert_levels:
                cur.execute("""
                    INSERT INTO alerts (symbol, alert_type, alert_direction, alert_level)
                    VALUES (%s, %s, %s, %s);
                """, (data['symbol'], data['alert_type'], data['alert_direction'], level.strip()))
            cur.execute("SELECT update_alert_notes();")
            cur.execute("COMMIT;")
        return jsonify({"message": "Alerts created successfully"}), 201
    except psycopg2.DatabaseError as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        connection_pool.putconn(conn)

@app.route('/alerts/<int:id>', methods=['PUT'])
def update_alert(id):
    data = request.get_json()
    conn = connection_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE alerts SET symbol=%s, alert_type=%s, alert_direction=%s, alert_level=%s, note=%s
                WHERE id=%s;
            """, (data['symbol'], data['alert_type'], data['alert_direction'], data['alert_level'], data['note'], id))
            conn.commit()
        return jsonify({"message": "Alert updated successfully"}), 200
    finally:
        cur.close()
        connection_pool.putconn(conn)

@app.route('/alerts/<int:id>', methods=['DELETE'])
def delete_alert(id):
    conn = connection_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM alerts WHERE id=%s;", [id])
            conn.commit()
        return jsonify({"message": "Alert deleted successfully"}), 200
    finally:
        cur.close()
        connection_pool.putconn(conn)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5005)
