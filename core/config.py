import os
from pathlib import Path

# --- Paths ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
INCOMING_DIR = DATA_DIR / "incoming"
PROCESSED_DIR = DATA_DIR / "processed"

# --- Database ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "bgd")
DB_USER = os.environ.get("DB_USER", "bgd")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "bgd")

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
)

# --- Embedding model ---
MODEL_NAME = "mixedbread-ai/mxbai-embed-xsmall-v1"
EMBEDDING_MAX_DIM = 384
MATRYOSHKA_DIMS = [384, 192, 96, 48]
EMBEDDING_BATCH_SIZE = 256

# --- Pipeline ---
BRONZE_BATCH_SIZE = 1000  # rows per INSERT batch for bronze ingestion
SILVER_BATCH_SIZE = 1000
GOLD_BATCH_SIZE = 256     # matches embedding batch size

# --- Kafka ---
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "bgd-incoming-files")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "bgd-pipeline")
