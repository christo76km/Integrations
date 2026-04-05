import requests
from pymongo import MongoClient
import time
# Configuration
LASTFM_API_KEY = "a5529673c7042a37aa417b16d0b34b8e"
LASTFM_USER = "christo761"
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "MusicLibrary"
MONGO_COLLECTION_NAME = "user_plays"
PAGE_LIMIT = 200  # Max number of results per page (200 is Last.fm's limit)
#%%
# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]
collection = db[MONGO_COLLECTION_NAME]
#%%
def fetch_lastfm_scrobbles(user, api_key, page=1, limit=PAGE_LIMIT):
    url = "https://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "user.getrecenttracks",
        "user": user,
        "api_key": api_key,
        "format": "json",
        "limit": limit,
        "page": page
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")
        return None
#%%
def save_to_mongo(data):
    if data:
        collection.insert_many(data)
        print(f"Inserted {len(data)} records into MongoDB.")
#%%
def main():
    print("Starting the script...")
    page = 1
    total_pages = 1  # Assume at least one page initially

    while page <= total_pages:
        print(f"Fetching page {page} of {total_pages}...")
        response = fetch_lastfm_scrobbles(LASTFM_USER, LASTFM_API_KEY, page)
        if response:
            tracks = response.get("recenttracks", {}).get("track", [])
            if tracks:
                # Transform data as needed before inserting into MongoDB
                transformed_data = [
                    {
                        "artist": track["artist"]["#text"],
                        "track": track["name"],
                        "album": track.get("album", {}).get("#text", ""),
                        "timestamp": track.get("date", {}).get("uts", None)
                    }
                    for track in tracks
                ]
                save_to_mongo(transformed_data)
            total_pages = int(response.get("recenttracks", {}).get("@attr", {}).get("totalPages", 1))
        else:
            print("Error fetching data. Exiting...")
            break
        page += 1
        time.sleep(1)  # To avoid hitting API rate limits

    print("Script completed.")
#%%
main()
