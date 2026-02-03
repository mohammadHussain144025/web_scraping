# src/scraper_agkits.py
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
from utils.utils import (
    get_xpath_combined, get_xpath_first, 
    random_sleep, match_from_description, remove_punctuation
)
from config.settings import AGKITS_HEADLESS, AGKITS_WAIT, AGKITS_BATCH_SIZE, AGKITS_THREADS
from utils.constants import oem_list, c_c_p_all, dis_all 

SITE_CODE = "AGKITS"
SITE_NAME = "ag_kits"
BASE_URL = "https://www.agkits.com"
PAGINATION_URL = "https://www.agkits.com/cummins-engines.aspx?size=200&page={}"

class AgKitsScraper:
    def __init__(self, config):
        self.config = config

    def legacy_get_part(self, description):
        """Original script logic preserved."""
        if not description: return ""
        out = ""
        parts = description.split(",")
        for p in parts:
            if 'RP' in p and 'RPM' not in p:
                out = p.strip()
        if out and "-" not in out:
            return out
        elif out:
            return out.split("-")[0].strip()
        return ""

    def scrape_item_worker(self, item_url, cat_url, scraper, logger):
        """Worker thread for individual engine page scraping."""
        driver = create_driver(headless=AGKITS_HEADLESS, wait_time=AGKITS_WAIT)
        try:
            logger.info(f"ITEM URL: {item_url}")
            driver.get(item_url)
            
            # 1. Broader Wait: Wait for either the Title or the SKU to appear
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, "//h1 | //span[contains(@id, 'lblProductSKU')]"))
                )
            except TimeoutException:
                logger.error(f"Timeout: Product components did not load for {item_url}. The site may be blocking.")
                return
            
            tree = html.fromstring(driver.page_source)

            # 2. Robust Extraction
            title = get_xpath_first(tree, '//h1/text()')
            sku = get_xpath_first(tree, '//span[contains(@id, "lblProductSKU")]/text()') or \
                  get_xpath_first(tree, '//span[contains(@class, "product-sku")]/text()')
            
            price = get_xpath_combined(tree, '//span[contains(@class, "product-detail-cost-value")]//text()') or \
                    get_xpath_combined(tree, '//span[contains(@id, "lblProductPrice")]//text()')

            # --- Quantity Extraction (ID from your screenshot) ---
            quantity = get_xpath_first(tree, '//input[@id="ctl00_pageContent_txtQuantity"]/@value') or "1"
            
            raw_desc = get_xpath_combined(tree, '//div[contains(@class, "product-detail-text")]//text()')
            full_data = f"{title} {raw_desc}"

            # Legacy matching logic
            clean_oem = ""
            for oem in oem_list:
                if oem.lower() in full_data.lower():
                    clean_oem = oem
                    break
            
            clean_part = self.legacy_get_part(full_data)
            engine_model = match_from_description(full_data, c_c_p_all)
            displacement = match_from_description(full_data, dis_all)

            scraper.add_row({
                "Page URL": cat_url,
                "Item URL": item_url,
                "OEM Name": "AG Kits (Reviva)",
                "Item Number": sku,
                "Item Description": title,
                "Price": price,
                "Quantity": quantity,
                "Currency": "USD",
                "Clean Part Number": clean_part if clean_part else sku,
                "Clean OEM": clean_oem,
                "Clean Part Description": title,
                "Engine Model": engine_model,
                "Displacement": displacement,
                "Source Name": "AG Kits (Reviva)",
                "Raw Data": raw_desc
            })
            logger.info(f"   SUCCESS -> SKU: {sku} | Price: {price} | Qty: {quantity}")

        except Exception as e:
            logger.error(f"Error at {item_url}: {str(e)[:100]}")
        finally:
            driver.quit()

    def run(self):
        logger = get_logger(SITE_NAME)
        scraper = BaseScraper(SITE_NAME, AGKITS_BATCH_SIZE, logger)
        nav_driver = create_driver(headless=AGKITS_HEADLESS, wait_time=AGKITS_WAIT)

        logger.info(f"STARTING MULTI-THREADED AG KITS SCRAPER")

        try:
            page = 1
            while True:
                url = PAGINATION_URL.format(page)
                logger.info(f"FETCHING PAGE: {url}")
                nav_driver.get(url)
                random_sleep(3, 5)

                tree = html.fromstring(nav_driver.page_source)
                # Broader product link discovery
                links = tree.xpath('//div[contains(@class, "product-list-item")]//h5/a/@href')

                if not links:
                    logger.info("No more engines found. Finalizing.")
                    break

                logger.info(f"Found {len(links)} engines. Processing via {AGKITS_THREADS} threads...")
                
                full_urls = [BASE_URL + l if l.startswith('/') else l for l in links]

                with ThreadPoolExecutor(max_workers=AGKITS_THREADS) as executor:
                    for item_url in full_urls:
                        executor.submit(self.scrape_item_worker, item_url, url, scraper, logger)
                
                page += 1
                if page > 50: break
        finally:
            nav_driver.quit()
            scraper.finalize()