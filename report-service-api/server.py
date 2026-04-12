#!/usr/bin/env python3
import http.server
import os
import jwt
from clickhouse_connect import get_client


CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST')
CLICKHOUSE_PORT = os.environ.get('CLICKHOUSE_PORT')
CLICKHOUSE_USER = os.environ.get('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD')
CLICKHOUSE_DATABASE = os.environ.get('CLICKHOUSE_DATABASE')

class ReportHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != '/reports':
            self.send_error(404, 'Not Found')
            return
        token = self.headers.get('Authorization')
        decoded = jwt.decode(token.removeprefix('Bearer '), options={"verify_signature": False})
        user = decoded['email']
        query = f"SELECT prosthesis_type, muscle_group, signal_frequency, signal_duration, signal_amplitude, signal_time FROM customer_reports WHERE email = '{user}'"

        try:
            csv_data = self._execute_clickhouse_query(query)
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
        client = get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password='',
            database=CLICKHOUSE_DATABASE)

        result = client.raw_stream(query=query, fmt='CSVWithNames')
        csv_chunks = []
        for chunk in result:
            csv_chunks.append(chunk.decode('utf-8'))

        final_csv_string = "".join(csv_chunks)
        return final_csv_string

def run_server(port: int = 8080):
    server_address = ('', port)
    httpd = http.server.HTTPServer(server_address, ReportHandler)
    print(f'Сервер запущен на порту {port}')
    print(f'ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}, база: {CLICKHOUSE_DATABASE}')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print('\nОстановка сервера...')
        httpd.server_close()

if __name__ == '__main__':
    run_server()
