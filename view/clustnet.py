import psycopg2
from flask import Flask, jsonify

app = Flask(__name__)

def get_cluster_data(cluster_id):
    dsn = "dbname=test user=postgres password=secret"
    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as cur:
                query = """
                    SELECT cluster_id, cluster_name, data_points
                    FROM clusters
                    WHERE cluster_id = %s
                """
                cur.execute(query, (cluster_id,))
                result = cur.fetchone()
                if result:
                    return {
                        'cluster_id': result[0],
                        'cluster_name': result[1],
                        'data_points': result[2]
                    }
                else:
                    return None
    finally:
        conn.close()

@app.route('/cluster/<int:cluster_id>')
def cluster(cluster_id):
    data = get_cluster_data(cluster_id)
    if data:
        return jsonify(data)
    else:
        return jsonify({'error': 'Cluster not found'}), 404

if __name__ == '__main__':
    app.run(debug=True)
