from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.db.session import engine, Base

from .api import attp, upload, ml, logs, sources

Base.metadata.create_all(bind=engine)

app = FastAPI(title="DX-Insights API", version="0.1.0")

origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
)


app.include_router(ml.router, prefix="/ml", tags=["ml"])
app.include_router(attp.router, prefix="/attp", tags=["attp"])
app.include_router(upload.router, prefix="/upload", tags=["upload"])
app.include_router(logs.router, prefix="/logs", tags=["logs"])
app.include_router(sources.router, prefix="/sources", tags=["sources"])

