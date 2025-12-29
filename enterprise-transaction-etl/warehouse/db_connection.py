import sqlite3
from pathlib import Path
import yaml

def get_db_connection():
    with open("config/settings.yaml", "r") as f:
        config = yaml.safe_load(f)

    db_path = Path(config["warehouse"]["database"])
    db_path.parent.mkdir(parents=True, exist_ok=True)

    return sqlite3.connect(db_path)
