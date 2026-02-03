# src/scraper_finditparts.py
import pandas as pd
import time
from lxml import html
from concurrent.futures import ThreadPoolExecutor
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from utils.selenium_factory import create_driver
from utils.base_scraper import BaseScraper
from utils.logger import get_logger
from utils.utils import get_xpath_combined, get_xpath_first
from config.settings import (
    FINDIT_HEADLESS, FINDIT_WAIT, 
    FINDIT_BATCH_SIZE, FINDIT_THREADS, 
    FINDIT_INPUT_FILE, RESOURCES_DIR
)

SITE_CODE = "FINDIT"
SITE_NAME = "find_it_parts"

class FindItPartsScraper:
    def __init__(self, config):
        self.config = config
        # Construct path strictly from .env variables
        self.input_path = RESOURCES_DIR / FINDIT_INPUT_FILE

    def scrape_item_worker(self, item_url, scraper, logger):
        """Worker thread for parallel URL processing."""
        driver = create_driver(headless=FINDIT_HEADLESS, wait_time=FINDIT_WAIT)
        try:
            logger.info(f"FETCHING ITEM: {item_url}")
            driver.get(item_url)
            
            # Wait for the quantity wrapper or order form seen in your screenshot
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, "//input[@id='order_quantity'] | //h1"))
                )
            except TimeoutException:
                logger.error(f"Timeout: Page components not found for {item_url}")
                return

            tree = html.fromstring(driver.page_source)

            # 1. Title
            title = get_xpath_first(tree, "//h1/text()")
            
            # 2. Price (Targeting Tailwind cent/dollar structure)
            price = get_xpath_first(tree, "//span[contains(@class, 'price_tag')]/@aria-label")
            if not price:
                price = get_xpath_combined(tree, "//*[contains(@class, 'price')]//text()")

            # 3. Quantity (Targeting the id from your snippet)
            quantity = get_xpath_first(tree, "//input[@id='order_quantity']/@value") or "1"

            # 4. SKU / Part Number
            sku = get_xpath_first(tree, "//span[contains(@class, 'sku')]//text()") or \
                  get_xpath_first(tree, "//div[contains(@class, 'productSKU')]//text()")

            # Robust Parsing for Manufacturer/Part Number
            oem_name = ""
            part_number = sku
            
            if title:
                # Handle "CASE 73131703 TURBINE" style (no dashes)
                title_parts = title.strip().split()
                if len(title_parts) >= 1:
                    oem_name = title_parts[0] # "CASE"
                if len(title_parts) >= 2 and not part_number:
                    part_number = title_parts[1] # "73131703"

            scraper.add_row({
                "Item URL": item_url,
                "Item description": title,
                "Price": price,
                "Quantity": quantity,
                "Item number": part_number,
                "Manufacturer Name": oem_name,
                "Currency": "USD",
                "Source Name": "FindItParts",
                "Raw Data": get_xpath_combined(tree, "//div[contains(@class, 'product-description')]//text()")
            })
            
            logger.info(f"   SUCCESS -> SKU: {part_number} | Price: {price} | Qty: {quantity}")

        except Exception as e:
            logger.error(f"Error at {item_url}: {str(e)[:100]}")
        finally:
            driver.quit()

    def run(self):
        logger = get_logger(SITE_NAME)
        
        if not self.input_path.exists():
            logger.error(f"CRITICAL: Input file missing at {self.input_path}")
            return

        try:
            findit_df = pd.read_excel(self.input_path, sheet_name='item url')
            source_urls = findit_df['item_url'].dropna().tolist()
            logger.info(f"Loaded {len(source_urls)} URLs from Excel.")
        except Exception as e:
            logger.error(f"Failed to read input Excel: {e}")
            return

        scraper = BaseScraper(SITE_NAME, FINDIT_BATCH_SIZE, logger)
        logger.info(f"STARTING FINDIT SCRAPER (Threads: {FINDIT_THREADS})")

        with ThreadPoolExecutor(max_workers=FINDIT_THREADS) as executor:
            for url in source_urls:
                executor.submit(self.scrape_item_worker, url, scraper, logger)
        
        scraper.finalize()