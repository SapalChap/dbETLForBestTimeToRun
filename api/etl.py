from http.server import BaseHTTPRequestHandler
from supabase_client import run_etl

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            result = run_etl()
            print("ETL run successful:", result)  # Logs in Vercel console
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write('{"status": "success", "result": "ETL completed"}'.encode())
            
        except Exception as e:
            print("ETL run failed:", e)
            
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(f'{{"status": "error", "message": "{str(e)}"}}'.encode())