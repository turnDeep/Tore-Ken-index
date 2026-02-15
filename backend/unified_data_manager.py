from datetime import datetime
import json
import os
import logging
from typing import Dict, Any, Optional

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.getcwd(), 'data')
UNIFIED_JSON_PATH = os.path.join(DATA_DIR, 'market_analysis.json')

def load_unified_data() -> Dict[str, Any]:
    """Loads the unified market analysis data."""
    if not os.path.exists(UNIFIED_JSON_PATH):
        return {}
    try:
        with open(UNIFIED_JSON_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load unified data: {e}")
        return {}

def save_unified_data(data: Dict[str, Any]):
    """Saves the unified market analysis data."""
    try:
        # Ensure directory exists
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(UNIFIED_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        logger.info(f"Unified data saved to {UNIFIED_JSON_PATH}")
    except Exception as e:
        logger.error(f"Failed to save unified data: {e}")

def update_ticker_data(ticker: str, section: str, data: Any):
    """
    Updates a specific section (short_term or long_term) for a ticker.
    preserves other sections.
    """
    unified_data = load_unified_data()

    if ticker not in unified_data:
        unified_data[ticker] = {}

    # Update the specific section
    unified_data[ticker][section] = {
        "data": data,
        "last_updated": datetime.now().isoformat()
    }

    save_unified_data(unified_data)

def get_ticker_data(ticker: str) -> Optional[Dict[str, Any]]:
    """Retrieves data for a specific ticker."""
    unified_data = load_unified_data()
    return unified_data.get(ticker)
