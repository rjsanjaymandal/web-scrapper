import os
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TestCancellation")

def simulate_stop_signal_sqlite():
    logger.info("Simulating STOP signal in SQLite...")
    import sqlite3
    db_path = Path(__file__).parent.parent / 'scraper_local.db'
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    # Eager schema creation if not exists
    cur.execute("CREATE TABLE IF NOT EXISTS system_status (id INTEGER PRIMARY KEY, key TEXT UNIQUE, value TEXT, updated_at TIMESTAMP)")
    
    stop_data = json.dumps({"running": False, "message": "Scraper STOP signal sent"})
    cur.execute("INSERT OR REPLACE INTO system_status (id, key, value, updated_at) VALUES (1, 'scraper_status', ?, NULL)", (stop_data,))
    conn.commit()
    cur.close()
    conn.close()
    logger.info("STOP signal recorded in SQLite.")

def simulate_running_signal_sqlite():
    logger.info("Simulating RUNNING signal in SQLite...")
    import sqlite3
    db_path = Path(__file__).parent.parent / 'scraper_local.db'
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS system_status (id INTEGER PRIMARY KEY, key TEXT UNIQUE, value TEXT, updated_at TIMESTAMP)")
    
    running_data = json.dumps({"running": True, "message": "Scraper is actively crawling"})
    cur.execute("INSERT OR REPLACE INTO system_status (id, key, value, updated_at) VALUES (1, 'scraper_status', ?, NULL)", (running_data,))
    conn.commit()
    cur.close()
    conn.close()
    logger.info("RUNNING signal recorded in SQLite.")

def test_fetcher():
    import sys
    sys.path.append(str(Path(__file__).parent.parent))
    from direct_scraper import DirectPoliteFetcher
    
    fetcher = DirectPoliteFetcher()
    
    # 1. Test when running is True
    simulate_running_signal_sqlite()
    logger.info("Testing fetch when running is True...")
    try:
        html, status = fetcher.fetch("https://httpbin.org/get")
        logger.info(f"Fetch succeeded! Status code: {status}")
    except KeyboardInterrupt:
        logger.error("Fetch unexpectedly aborted!")
        return False
        
    # 2. Test when running is False
    simulate_stop_signal_sqlite()
    logger.info("Testing fetch when running is False (should abort)...")
    try:
        fetcher.fetch("https://httpbin.org/get")
        logger.error("Fetch unexpectedly succeeded instead of aborting!")
        return False
    except KeyboardInterrupt as ki:
        logger.info(f"SUCCESS! Fetch aborted exactly as expected: {ki}")
        return True

if __name__ == "__main__":
    success = test_fetcher()
    if success:
        logger.info("ALL CANCELLATION TESTS PASSED SUCCESSFULLY!")
    else:
        logger.error("CANCELLATION TESTS FAILED!")
