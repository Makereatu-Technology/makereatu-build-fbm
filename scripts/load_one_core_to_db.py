import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.services.db.load_core_json import load_core_json_to_postgres

# UPDATE THESE
CORE_JSON_PATH = r"F:\D\Makereatu AI\Updated PAD_IEG_WB_ADB\_artifacts\_core_extraction\9148e6dd9163416a57809e66bee7224a92b57d513febbcda2d01b5ce7d3258ba.core.json"
DATABASE_URL = "postgresql+psycopg2://postgres:kemboi@localhost:5432/makereatu_fbm"

result = load_core_json_to_postgres(CORE_JSON_PATH, DATABASE_URL)
print(result)