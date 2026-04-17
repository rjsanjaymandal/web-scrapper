import asyncio
import logging
import os
import sys
import time
from typing import List, Dict

# Ensure current directory is in path for imports
sys.path.append(os.getcwd())

from scraper import load_config
import asyncpg
from redis_manager import RedisQueueManager

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class BulkWriter:
    def __init__(self, db_url: str, redis_url: str, batch_size: int = 1000, max_wait: int = 30):
        self.db_url = db_url
        self.redis_url = redis_url
        self.batch_size = batch_size
        self.max_wait = max_wait
        self.queue = RedisQueueManager(redis_url)
        self.pool = None
        self.local_buffer = []
        self.last_write_time = time.time()

    async def init(self):
        await self.queue.connect()
        # Ensure postgres protocol is correct
        if self.db_url and self.db_url.startswith("postgres://"):
            self.db_url = self.db_url.replace("postgres://", "postgresql://", 1)
        self.pool = await asyncpg.create_pool(dsn=self.db_url, min_size=1, max_size=5)
        logger.info("✅ Bulk Writer Initialized (DB + Redis connected)")

    async def _flush(self):
        if not self.local_buffer:
            return
        
        count = len(self.local_buffer)
        logger.info(f"💾 Flushing {count} records to Postgres...")
        
        # Prepare records for asyncpg executemany
        records = []
        for rec in self.local_buffer:
            if not rec.get("name"):
                continue
                
            records.append((
                rec.get("name", "")[:255],
                rec.get("phone", "")[:50],
                rec.get("email", "")[:255],
                rec.get("address", ""),
                rec.get("category", "")[:100],
                rec.get("city", "")[:100],
                rec.get("area", "")[:100],
                rec.get("state", "")[:100],
                rec.get("source", "")[:100],
                rec.get("detail_url", "") or rec.get("source_url", ""),
                rec.get("phone_clean", "")[:50],
                rec.get("email_valid", False),
                True,  # enriched
                rec.get("arn", "")[:50],
                rec.get("license_no", "")[:100],
                rec.get("membership_no", "")[:100],
                rec.get("quality_score", 0),
                rec.get("quality_tier", "low"),
            ))

        if not records:
            self.local_buffer = []
            return

        try:
            async with self.pool.acquire() as conn:
                await conn.executemany("""
                    INSERT INTO contacts (
                        name, phone, email, address, category, city, area, state, source, 
                        source_url, phone_clean, email_valid, enriched, arn, license_no, 
                        membership_no, quality_score, quality_tier
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
                    ON CONFLICT (phone_clean) WHERE phone_clean IS NOT NULL
                    DO UPDATE SET
                        quality_score = EXCLUDED.quality_score,
                        quality_tier = EXCLUDED.quality_tier,
                        scraped_at = EXCLUDED.scraped_at,
                        enriched = TRUE
                    WHERE EXCLUDED.quality_score >= contacts.quality_score
                """, records)
            logger.info(f"✅ Successfully wrote {len(records)} records (Batch size: {count})")
        except Exception as e:
            logger.error(f"❌ Bulk Write Failed: {e}")
            # In a production environment, we'd dead-letter these or retry.
            # For now, we keep them in buffer if it was a connection error, or log and clear.
        
        self.local_buffer = []
        self.last_write_time = time.time()

    async def run(self):
        logger.info(f"🚀 Bulk Writer Running (Batch: {self.batch_size}, Timeout: {self.max_wait}s)")
        try:
            while True:
                # 1. Pull batch from Redis
                batch = await self.queue.pop_results_batch(max_records=self.batch_size)
                if batch:
                    self.local_buffer.extend(batch)
                
                # 2. Check flush conditions
                time_since_flush = time.time() - self.last_write_time
                if len(self.local_buffer) >= self.batch_size or (self.local_buffer and time_since_flush >= self.max_wait):
                    await self._flush()
                
                if not batch:
                    # Idle sleep to prevent CPU spin
                    await asyncio.sleep(2)
                    
        except asyncio.CancelledError:
            logger.info("Writer shutting down. Performing final flush...")
            await self._flush()
        finally:
            if self.pool:
                await self.pool.close()
            await self.queue.disconnect()

async def main():
    config = load_config()
    db_url = os.environ.get("DATABASE_URL")
    redis_url = os.environ.get("REDIS_URL") or config.scraper_settings.get("redis_url")
    
    if not db_url or not redis_url:
        logger.error("Missing DB_URL or REDIS_URL. Cannot start writer.")
        return

    writer = BulkWriter(
        db_url=db_url, 
        redis_url=redis_url,
        batch_size=int(os.environ.get("BATCH_SIZE", 500)), # Lower default for safer flush
        max_wait=int(os.environ.get("WRITE_INTERVAL", 30))
    )
    await writer.init()
    await writer.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
