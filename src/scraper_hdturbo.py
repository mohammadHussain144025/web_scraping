# src/scraper_hdturbo.py
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
from config.settings import HDTURBO_HEADLESS, HDTURBO_WAIT, HDTURBO_BATCH_SIZE, HDTURBO_THREADS
from utils.constants import oem_list, c_c_p_all

SITE_CODE = "HDTURBO"
SITE_NAME = "hd_turbo"
START_URL_TEMPLATE = "https://hdturbo.com/buy-turbo-store/?product-page={}"

class HDTurboScraper:
    def __init__(self, config):
        self.config = config

    def close_modals(self, driver):
        """Closes popups that might block rendering."""
        try:
            # Common close buttons for WordPress/WooCommerce popups
            close_selectors = [
                "//button[contains(@class, 'pum-close')]",
                "//button[@aria-label='Close']",
                "//span[contains(@class, 'close')]"
            ]
            for selector in close_selectors:
                elements = driver.find_elements(By.XPATH, selector)
                for el in elements:
                    if el.is_displayed():
                        el.click()
        except:
            pass

    def scrape_item_worker(self, item_url, cat_url, scraper, logger):
        """Worker thread to process individual product pages."""
        driver = create_driver(headless=HDTURBO_HEADLESS, wait_time=HDTURBO_WAIT)
        try:
            logger.info(f"ITEM URL: {item_url}")
            driver.get(item_url)
            
            # Close any popups that might have appeared
            self.close_modals(driver)

            # Wait for content load - broad wait (Title or Summary)
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, "//h1 | //div[contains(@class, 'summary')]"))
                )
            except TimeoutException:
                logger.error(f"Timeout: Product components didn't load for {item_url}")
                return
            
            tree = html.fromstring(driver.page_source)

            # --- Extraction Logic ---
            title = get_xpath_first(tree, '//h1[contains(@class, "product_title")]/text()')
            sku = get_xpath_first(tree, '//span[@class="sku"]/text()') or \
                  get_xpath_first(tree, '//span[contains(@class, "product_id")]/text()')
            
            price = get_xpath_combined(tree, '//p[contains(@class, "price")]//span[contains(@class, "amount")]//text()')
            
            # Quantity Extraction (Targets name="quantity" from your screenshot)
            quantity = get_xpath_first(tree, '//input[@name="quantity"]/@value') or "1"
            
            raw_data = get_xpath_combined(tree, '//div[contains(@class, "summary")]//text()')

            found_oem = match_from_description(title, oem_list)
            found_eng = match_from_description(raw_data, c_c_p_all)
            
            clean_part = title.split()[0].strip() if title else ""
            clean_desc = remove_punctuation(title.replace(clean_part, "")) if title else ""

            scraper.add_row({
                "Page URL": cat_url,
                "Item URL": item_url,
                "Item description": title,
                "Part Number": sku if sku else clean_part,
                "OEM Name": found_oem,
                "Engine Model": found_eng,
                "Price": price,
                "Quantity": quantity,
                "Currency": "USD",
                "Clean OEM": found_oem,
                "Clean Part Number": sku if sku else clean_part,
                "Clean Part Description": clean_desc,
                "Source Name": "HD Turbo",
                "Raw Data": raw_data
            })
            logger.info(f"   SUCCESS -> SKU: {sku} | Qty: {quantity} | Price: {price}")
        except Exception as e:
            logger.error(f"Error scraping {item_url}: {str(e)[:100]}")
        finally:
            driver.quit()

    def run(self):
        logger = get_logger(SITE_NAME)
        scraper = BaseScraper(SITE_NAME, HDTURBO_BATCH_SIZE, logger)
        nav_driver = create_driver(headless=HDTURBO_HEADLESS, wait_time=HDTURBO_WAIT)

        logger.info(f"STARTING MULTI-THREADED SCRAPER: {SITE_NAME}")

        try:
            page = 1
            while True:
                url = START_URL_TEMPLATE.format(page)
                logger.info(f"FETCHING CATEGORY PAGE: {url}")
                
                nav_driver.get(url)
                random_sleep(3, 5)

                tree = html.fromstring(nav_driver.page_source)
                # Broader WooCommerce product link selector
                links = tree.xpath("//li[contains(@class, 'product')]//a[contains(@class, 'link')]/@href") or \
                        tree.xpath("//ul[contains(@class, 'products')]//li//a[1]/@href")

                if not links:
                    logger.info("No more products found.")
                    break

                logger.info(f"Processing {len(links)} items via {HDTURBO_THREADS} threads...")

                with ThreadPoolExecutor(max_workers=HDTURBO_THREADS) as executor:
                    for item_url in links:
                        # Skip pagination links that might get caught
                        if "product-page=" in item_url: continue
                        executor.submit(self.scrape_item_worker, item_url, url, scraper, logger)
                
                page += 1
                if page > 50: break 
        finally:
            nav_driver.quit()
            scraper.finalize()