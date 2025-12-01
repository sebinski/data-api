from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, text, select
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool
from pydantic import BaseModel
import os
import logging

from .models import Base, Item

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST", "my-mariadb.database.svc.cluster.local")
DB_USER = os.getenv("DB_USER", "apiuser")
DB_PASS = os.getenv("DB_PASS", "ApiUserPass123")
DB_NAME = os.getenv("DB_NAME", "apidb")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:3306/{DB_NAME}"
logger.info(f"Using DB: {DATABASE_URL}")

# Use NullPool for simplicity (each request gets a fresh connection)
engine = create_engine(DATABASE_URL, poolclass=NullPool, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

app = FastAPI(title="Data API", version="1.0.0")


class ItemCreate(BaseModel):
    name: str
    description: str | None = None


class ItemRead(BaseModel):
    id: int
    name: str
    description: str | None
    created_at: str

    class Config:
        orm_mode = True


class ItemUpdate(BaseModel):
    name: str | None = None
    description: str | None = None


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.on_event("startup")
def startup():
    logger.info("Creating tables if they do not exist...")
    Base.metadata.create_all(engine)
    logger.info("Startup complete.")


@app.get("/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "unhealthy", "error": str(e)}


@app.get("/")
def root():
    return {"message": "Data API running"}


@app.post("/items", response_model=ItemRead, status_code=201)
def create_item(payload: ItemCreate, db: Session = Depends(get_db)):
    # Check duplicate name
    existing = db.execute(select(Item).where(Item.name == payload.name)).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="Item name already exists")
    item = Item(name=payload.name, description=payload.description)
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@app.get("/items", response_model=list[ItemRead])
def list_items(db: Session = Depends(get_db)):
    result = db.execute(select(Item).order_by(Item.id.desc()))
    return result.scalars().all()


@app.get("/items/{item_id}", response_model=ItemRead)
def get_item(item_id: int, db: Session = Depends(get_db)):
    item = db.get(Item, item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return item


@app.patch("/items/{item_id}", response_model=ItemRead)
def update_item(item_id: int, payload: ItemUpdate, db: Session = Depends(get_db)):
    item = db.get(Item, item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    if payload.name is not None:
        # Check duplicate
        dup = db.execute(select(Item).where(Item.name == payload.name, Item.id != item_id)).scalar_one_or_none()
        if dup:
            raise HTTPException(status_code=409, detail="Name already used by another item")
        item.name = payload.name
    if payload.description is not None:
        item.description = payload.description
    db.commit()
    db.refresh(item)
    return item


@app.delete("/items/{item_id}", status_code=204)
def delete_item(item_id: int, db: Session = Depends(get_db)):
    item = db.get(Item, item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    db.delete(item)
    db.commit()
    return None
