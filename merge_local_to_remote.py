#!/usr/bin/env python3
"""
Standalone script: Merge local SQLite contacts into remote PostgreSQL.
Optimized to use the new exception-free, bidirectional synchronization engine.
"""

import sys
import asyncio
from merge_db import run_sync

if __name__ == "__main__":
    try:
        asyncio.run(run_sync())
    except KeyboardInterrupt:
        print("\n👋 Sync interrupted by user. Exiting.")
        sys.exit(0)
