# src/scraper_vanderhaags.py
import time
from lxml import html
from concurrent.futures import ThreadPoolExecutor
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from utils.selenium_factory import create_driver
from utils.base_scraper import BaseScraper
from utils.logger import get_logger
from utils.utils import (
    get_xpath_combined, get_xpath_first, 
    random_sleep, remove_punctuation,
    extract_vander_part, extract_vander_oem
)
from config.settings import VANDERHAAGS_HEADLESS, VANDERHAAGS_WAIT, VANDERHAAGS_BATCH_SIZE, VANDERHAAGS_THREADS

SITE_CODE = "VANDERHAAGS"
SITE_NAME = "vander_haags"
BASE_URL = "https://www.vanderhaags.com/"
LISTING_URL = "https://www.vanderhaags.com/Search-Unit.php?inventorytype=truck&items=500&order=Lowest+Price"

class VanderHaagsScraper:
    def __init__(self, config):
        self.config = config

    def scrape_item_worker(self, item_url, cat_url, scraper, logger):
        """Worker thread to process individual truck pages."""
        driver = create_driver(headless=VANDERHAAGS_HEADLESS, wait_time=VANDERHAAGS_WAIT)
        try:
            logger.info(f"ITEM URL: {item_url}")
            driver.get(item_url)
            
            # Wait for main description block
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, "//h1")))
            
            tree = html.fromstring(driver.page_source)

            # Core fields
            desc = get_xpath_combined(tree, '//h1[1]//text()')
            price = get_xpath_combined(tree, '//*[@class="item-price"]//text()')
            
            # Detailed Info Table Extraction (Legacy loop logic)
            table_data = tree.xpath('//*[@class="iteminfo"]//text() | //*[@id="panel2a"]//text() | //*[@class="accordion"]//text()')
            table_text = [t.strip() for t in table_data if t.strip()]
            
            info = {
                "Item #": "", "Sleeper": "", "Engine": "", 
                "Mileage": "", "VIN": "", "GVW": ""
            }

            for i, text in enumerate(table_text):
                if 'Item #:' in text: info["Item #"] = text.replace('Item #:', '').strip()
                elif text == 'Sleeper:' and i+1 < len(table_text): info["Sleeper"] = table_text[i+1]
                elif text == 'Engine:' and i+1 < len(table_text): info["Engine"] = table_text[i+1]
                elif text == 'Mileage:' and i+1 < len(table_text): info["Mileage"] = table_text[i+1]
                elif text == 'VIN:' and i+1 < len(table_text): info["VIN"] = table_text[i+1]
                elif text == 'GVW:' and i+1 < len(table_text): info["GVW"] = table_text[i+1]

            # Processing Logic
            part = extract_vander_part(desc)
            oem = extract_vander_oem(desc)
            clean_desc = desc.replace(str(oem), "").replace(str(part), "")
            clean_desc = remove_punctuation(clean_desc).strip()

            scraper.add_row({
                "Page URL": cat_url,
                "Item URL": item_url,
                "Item description": desc,
                "Price": price,
                "Currency": "USD",
                "Item #": info["Item #"],
                "Sleeper": info["Sleeper"],
                "Engine": info["Engine"],
                "Mileage": info["Mileage"],
                "VIN": info["VIN"],
                "GVW": info["GVW"],
                "Clean Part Number": part,
                "Clean OEM": oem,
                "Clean Part Description": clean_desc,
                "Source Name": "Vander Haags"
            })
            logger.info(f"   SUCCESS -> Item #: {info['Item #']} | Price: {price}")
        except Exception as e:
            logger.error(f"Error scraping {item_url}: {str(e)[:100]}")
        finally:
            driver.quit()

    def run(self):
        logger = get_logger(SITE_NAME)
        scraper = BaseScraper(SITE_NAME, VANDERHAAGS_BATCH_SIZE, logger)
        nav_driver = create_driver(headless=VANDERHAAGS_HEADLESS, wait_time=VANDERHAAGS_WAIT)

        logger.info(f"STARTING MULTI-THREADED SCRAPER: {SITE_NAME}")

        try:
            logger.info(f"FETCHING LISTING PAGE: {LISTING_URL}")
            nav_driver.get(LISTING_URL)
            random_sleep(4, 6)

            tree = html.fromstring(nav_driver.page_source)
            # Find relative URLs for items
            relative_links = tree.xpath('//div[@class="item-card "]//a/@href')
            
            # Resolve to full URLs and remove duplicates
            item_urls = list(set([BASE_URL + l.lstrip('/') for l in relative_links]))
            
            logger.info(f"Found {len(item_urls)} trucks to process...")

            # Run in parallel using threads from .env
            with ThreadPoolExecutor(max_workers=VANDERHAAGS_THREADS) as executor:
                for url in item_urls:
                    executor.submit(self.scrape_item_worker, url, LISTING_URL, scraper, logger)

        finally:
            nav_driver.quit()
            scraper.finalize()
            logger.info(f"FINISHED SCRAPER: {SITE_NAME}")