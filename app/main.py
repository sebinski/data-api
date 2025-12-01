from fastapi import FastAPI
from sqlalchemy import create_engine, text
import os

app = FastAPI()

DB_HOST = os.getenv("DB_HOST", "my-mariadb. database.svc.cluster.local")
DB_USER = os.getenv("DB_USER", "apiuser")
DB_PASS = os.getenv("DB_PASS", "ApiUserPass123")
DB_NAME = os.getenv("DB_NAME", "apidb")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:3306/{DB_NAME}"
engine = create_engine(DATABASE_URL)

@app.get("/")
def read_root():
    return {"message": "Data API is running"}

@app.get("/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.get("/data")
def get_data():
    try:
        with engine.connect() as conn:
            result = conn. execute(text("SELECT 1 as id, 'sample' as name"))
            rows = [dict(row._mapping) for row in result]
        return {"data": rows}
    except Exception as e:
        return {"error": str(e)}
