# utils/base_scraper.py
import pandas as pd
from config.settings import DATA_DIR, RUN_MONTH

class BaseScraper:
    def __init__(self, site_name: str, batch_size: int, logger):
        self.site_name = site_name
        self.batch_size = batch_size
        self.logger = logger
        self.rows = []
        self.total_processed = 0 

    def add_row(self, row: dict):
        self.rows.append(row)
        if len(self.rows) >= self.batch_size:
            self.flush_batch()

    def flush_batch(self):
        if not self.rows:
            return

        self.total_processed += len(self.rows)
        df = pd.DataFrame(self.rows)

        out_dir = DATA_DIR / f"{self.site_name}_{RUN_MONTH}"
        out_dir.mkdir(parents=True, exist_ok=True)

        batch_file = out_dir / f"batch_{self.total_processed}.csv"
        df.to_csv(batch_file, index=False)

        self.logger.info(f"SAVED BATCH: {batch_file.name} | Total rows: {self.total_processed}")
        self.rows = []

    def finalize(self):
        if self.rows:
            self.total_processed += len(self.rows)
            df = pd.DataFrame(self.rows)
            out_dir = DATA_DIR / f"{self.site_name}_{RUN_MONTH}"
            batch_file = out_dir / f"batch_{self.total_processed}.csv"
            df.to_csv(batch_file, index=False)
            self.logger.info(f"SAVED FINAL BATCH: {batch_file.name}")
        
        self.logger.info(f"SCRAPING COMPLETE: {self.site_name} | TOTAL RECORDS: {self.total_processed}")