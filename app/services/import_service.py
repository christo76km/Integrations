import os
from dotenv import load_dotenv
from pathlib import Path

from .lastfm_import import import_new_scrobbles

# Load .env once
load_dotenv()

def run_lastfm_import(db_path: str):
    username = os.getenv("LASTFM_USERNAME")
    api_key = os.getenv("LASTFM_API_KEY")

    if not username or not api_key:
        raise RuntimeError("LASTFM_USERNAME or LASTFM_API_KEY missing in .env")

    return import_new_scrobbles(
        db_path=db_path,
        api_key=api_key,
        username=username
    )