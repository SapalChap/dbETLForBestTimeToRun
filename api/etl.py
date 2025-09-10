from supabase_client import run_etl

def handler(request, response):
    try:
        result = run_etl()
        print("ETL run successful:", result)  # Logs in Vercel console
        return response.status(200).json({"status": "success", "result": str(result)})
    except Exception as e:
        print("ETL run failed:", e)
        return response.status(500).json({"status": "error", "message": str(e)})