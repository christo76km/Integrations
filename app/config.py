import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

class Config:
    DATABASE_URL = os.getenv("DATABASE_URL")
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key")