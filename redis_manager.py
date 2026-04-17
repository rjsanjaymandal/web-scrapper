import json
import os
import logging
from typing import Optional, Dict, List, Any
import redis.asyncio as redis

logger = logging.getLogger(__name__)

class RedisQueueManager:
    """
    Handles distributed task and result queues for the scraper.
    """
    TASKS_KEY = "scraper:tasks"
    RESULTS_KEY = "scraper:results_buffer"
    STATS_KEY = "scraper:stats"
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.client = None

    async def connect(self):
        if not self.client:
            self.client = redis.Redis.from_url(self.redis_url, decode_responses=True)
            await self.client.ping()
            logger.info("Connected to Redis successfully.")

    async def disconnect(self):
        if self.client:
            await self.client.close()
            self.client = None

    # --- Task Queue Operations ---
    async def push_task(self, source: str, city: str, category: str, page: int = 1):
        task = {
            "source": source.upper(),
            "city": city,
            "category": category,
            "page": page
        }
        await self.client.rpush(self.TASKS_KEY, json.dumps(task))
        # Update stats
        await self.client.hincrby(self.STATS_KEY, "total_tasks_pushed", 1)

    async def pop_task(self, timeout: int = 0) -> Optional[Dict]:
        """Blocking pop from the task queue."""
        result = await self.client.blpop(self.TASKS_KEY, timeout=timeout)
        if result:
            _, data = result
            return json.loads(data)
        return None

    # --- Result Buffer Operations ---
    async def push_results(self, results: List[Dict]):
        if not results:
            return
        # Store as a JSON list-string in the results buffer
        await self.client.rpush(self.RESULTS_KEY, json.dumps(results))
        # Update stats
        await self.client.hincrby(self.STATS_KEY, "total_records_scraped", len(results))

    async def pop_results_batch(self, max_records: int = 1000) -> List[Dict]:
        """
        Pops multiple items from the result buffer.
        Note: Items in the buffer are lists of contacts.
        """
        all_contacts = []
        # Attempt to get up to max_records, but don't block
        # We use a pipeline for performance
        pipe = self.client.pipeline()
        for _ in range(100): # Limit loop iterations to prevent long blocking
             pipe.lpop(self.RESULTS_KEY)
        
        results = await pipe.execute()
        
        for r in results:
            if r:
                all_contacts.extend(json.loads(r))
            
            if len(all_contacts) >= max_records:
                break
                
        return all_contacts

    async def get_queue_depth(self) -> Dict[str, int]:
        tasks = await self.client.llen(self.TASKS_KEY)
        results = await self.client.llen(self.RESULTS_KEY)
        return {"tasks": tasks, "results_buffer": results}
