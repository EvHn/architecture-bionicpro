#!/usr/bin/env python3
import http.server
import os
import jwt
from clickhouse_connect import get_client
from minio import Minio
from datetime import datetime, timedelta, timezone
from io import BytesIO

CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST')
CLICKHOUSE_PORT = os.environ.get('CLICKHOUSE_PORT')
CLICKHOUSE_USER = os.environ.get('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD')
CLICKHOUSE_DATABASE = os.environ.get('CLICKHOUSE_DATABASE')

clientminio = Minio(
    "minio:9000",
    access_key=os.environ.get('MINIO_USER'),
    secret_key=os.environ.get('MINIO_PASSWORD'),
    secure=False
)

clientch = get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password='',
            database=CLICKHOUSE_DATABASE)

class ReportHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != '/reports':
            self.send_error(404, 'Not Found')
            return
        token = self.headers.get('Authorization')
        decoded = jwt.decode(token.removeprefix('Bearer '), options={"verify_signature": False})
        user = decoded['email']
        try:
            csv_data = self.get_cached_report(user)
        except Exception as e:
            self.send_error(500, f'ClickHouse error: {str(e)}')
            return

        self.send_response(200)
        self.send_header('Content-Type', 'text/csv; charset=utf-8')
        self.send_header('Content-Disposition',
                         f'attachment; filename="report_{user}.csv"')
        self.end_headers()
        self.wfile.write(csv_data.encode('utf-8'))

    def _execute_clickhouse_query(self, query: str) -> str:
        result = clientch.raw_stream(query=query, fmt='CSVWithNames')
        csv_chunks = []
        for chunk in result:
            csv_chunks.append(chunk.decode('utf-8'))

        final_csv_string = "".join(csv_chunks)
        return final_csv_string
    
    def get_report(self, email):
        query = f"SELECT e.prosthesis_type, e.muscle_group, e.signal_frequency, e.signal_duration, e.signal_amplitude, e.signal_time FROM customers c LEFT JOIN emg_sensor_data e ON c.id = e.user_id WHERE email = '{email}'"
        return self._execute_clickhouse_query(query)

    def get_cached_report(self, user_id):
        bucket = 'reports'
        key = f"reports/{user_id}.csv"
        cutoff = datetime.now(timezone.utc) - timedelta(weeks=1)
        try:
            stat = clientminio.stat_object(bucket, key)
            print(stat)
            if stat.last_modified > cutoff:
                response = clientminio.get_object(bucket, key)
                data = response.read().decode('utf-8')
                response.close()
                response.release_conn()
                return data
            else:
                clientminio.remove_object(bucket, key)
        except Exception:
            pass

        report = self.get_report(user_id)

        data_bytes = report.encode('utf-8')
        data_stream = BytesIO(data_bytes)

        clientminio.put_object(
            bucket, key,
            data=data_stream,
            length=len(data_bytes),
            content_type='text/csv'
        )

        return report        
    
def run_server(port: int = 8080):
    server_address = ('', port)
    httpd = http.server.HTTPServer(server_address, ReportHandler)
    print(f'Сервер запущен на порту {port}')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print('\nОстановка сервера...')
        httpd.server_close()

if __name__ == '__main__':
    run_server()
