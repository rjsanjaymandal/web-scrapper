import os
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

class RawStorage:
    """Implements 'The Golden Rule': Save Raw HTML First."""
    
    def __init__(self, base_dir: str = "raw_data", retention_days: int = 7):
        self.base_dir = Path(base_dir)
        self.retention_days = retention_days
        self.base_dir.mkdir(exist_ok=True)
        
    def save(self, content: str, source: str, city: str, category: str) -> Optional[str]:
        """Save raw HTML content to a structured directory."""
        if not content:
            return None
            
        try:
            # Create sub-directory for the date
            date_str = datetime.now().strftime("%Y-%m-%d")
            save_dir = self.base_dir / date_str / source.lower()
            save_dir.mkdir(parents=True, exist_ok=True)
            
            # Create slug-friendly filename
            timestamp = datetime.now().strftime("%H%M%S")
            cat_slug = category.lower().replace(" ", "_")[:20]
            filename = f"{city}_{cat_slug}_{timestamp}.html"
            
            filepath = save_dir / filename
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)
            
            # Post-save: Cleanup old files (Ethical/Resource Management)
            self._cleanup()
            
            return str(filepath)
        except Exception as e:
            logger.error(f"Failed to save raw HTML: {e}")
            return None

    def _cleanup(self):
        """Delete folders older than retention_days."""
        try:
            cutoff = datetime.now() - timedelta(days=self.retention_days)
            for folder in self.base_dir.iterdir():
                if folder.is_dir():
                    try:
                        folder_date = datetime.strptime(folder.name, "%Y-%m-%d")
                        if folder_date < cutoff:
                            logger.info(f"💾 Cleanup: Removing expired raw data folder {folder.name}")
                            # Remove all files and the folder itself
                            self._rmtree(folder)
                    except ValueError:
                        continue # Not a date-formatted folder
        except Exception as e:
            logger.error(f"Storage cleanup error: {e}")

    def _rmtree(self, path: Path):
        """Recursively remove a directory."""
        for child in path.iterdir():
            if child.is_dir():
                self._rmtree(child)
            else:
                child.unlink()
        path.rmdir()

# Singleton instance
storage = RawStorage()
