# src/scraper_bigbear.py
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
    random_sleep, match_from_description
)
from config.settings import BIGBEAR_HEADLESS, BIGBEAR_WAIT, BIGBEAR_BATCH_SIZE, BIGBEAR_THREADS
from utils.constants import oem_list, c_c_p_all, dis_all 

SITE_CODE = "BIGBEAR"
SITE_NAME = "big_bear_engine"

class BigBearScraper:
    def __init__(self, config):
        self.base_urls = [
            'https://shop.4btengines.com/product-category/diesel-engines/complete-engines/',
            'https://shop.4btengines.com/product-category/diesel-engines/long-block-engines/',
            'https://shop.4btengines.com/product-category/engine-rebuild-kits/inframeoverhaul/',
            'https://shop.4btengines.com/product-category/engine-rebuild-kits/other-kits/'
        ]
        self.component_url = 'https://shop.4btengines.com/product-category/engine-components/'

    def get_sub_categories(self, logger):
        """Discovers sub-category links from the components page."""
        logger.info("Discovering sub-categories from Engine Components...")
        driver = create_driver(headless=BIGBEAR_HEADLESS, wait_time=BIGBEAR_WAIT)
        try:
            driver.get(self.component_url)
            random_sleep(4, 6)
            tree = html.fromstring(driver.page_source)
            links = tree.xpath('//li[contains(@class, "product-category")]//a/@href')
            if not links:
                links = tree.xpath('//ul[contains(@class, "products")]//a[contains(@href, "product-category")]/@href')
            links = list(set(links))
            logger.info(f"Discovered {len(links)} sub-categories.")
            return links
        except Exception as e:
            logger.error(f"Error discovering sub-categories: {e}")
            return []
        finally:
            driver.quit()

    def scrape_item_worker(self, item_url, cat_url, scraper, logger):
        driver = create_driver(headless=BIGBEAR_HEADLESS, wait_time=BIGBEAR_WAIT)
        try:
            driver.get(item_url)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".product_title, .entry-summary")))
            
            tree = html.fromstring(driver.page_source)
            
            title = get_xpath_first(tree, '//h1[contains(@class, "product_title")]/text()') or \
                    get_xpath_first(tree, '//section[@class="title-section"]//h1/text()')
            
            sku = get_xpath_first(tree, '//span[@class="sku"]/text()')
            price = get_xpath_combined(tree, '//p[contains(@class, "price")]//span[contains(@class, "amount")]//text()')
            
            # --- NEW: Quantity Extraction ---
            # Targets the input field shown in your screenshot
            quantity = get_xpath_first(tree, '//input[@name="quantity"]/@value') or "1"
            
            raw_data = get_xpath_combined(tree, '//div[contains(@class, "summary")]//text()')
            hp = get_xpath_first(tree, '//tr[contains(@class, "horsepower")]//td//text()')
            displacement = get_xpath_first(tree, '//tr[contains(@class, "displacement")]//td//text()')
            stock = get_xpath_first(tree, '//p[contains(@class, "stock")]//text()') or "In Stock"

            engine_model = match_from_description(title + " " + raw_data, c_c_p_all)
            clean_oem = title.split()[0] if title else ""

            scraper.add_row({
                "Page URL": cat_url,
                "Item URL": item_url,
                "Item description": title,
                "Sku": sku,
                "Price": price,
                "Quantity": quantity, # Added field
                "OEM Name": title,
                "Engine Model": engine_model,
                "Horse Power": hp,
                "Displacement": displacement,
                "Stock Availability": stock,
                "Currency": "USD",
                "Clean Part Description": title,
                "Clean OEM": clean_oem,
                "Clean Part Number": sku,
                "Source Name": "Big Bear Engine Company",
                "Raw Data": raw_data
            })
            logger.info(f"   SUCCESS -> SKU: {sku} | Qty: {quantity} | Price: {price}")
        except Exception as e:
            logger.error(f"Error scraping product: {item_url} | {e}")
        finally:
            driver.quit()

    def run(self):
        logger = get_logger(SITE_NAME)
        scraper = BaseScraper(SITE_NAME, BIGBEAR_BATCH_SIZE, logger)
        
        sub_cats = self.get_sub_categories(logger)
        all_categories = self.base_urls + sub_cats

        nav_driver = create_driver(headless=BIGBEAR_HEADLESS, wait_time=BIGBEAR_WAIT)
        logger.info(f"STARTING MULTI-THREADED SCRAPER: {SITE_NAME}")

        try:
            for cat_base_url in all_categories:
                page = 1
                while True:
                    if page == 1:
                        url = cat_base_url
                    else:
                        url = f"{cat_base_url.rstrip('/')}/page/{page}/"

                    logger.info(f"FETCHING CATEGORY PAGE: {url}")
                    nav_driver.get(url)
                    random_sleep(3, 5)

                    if "404" in nav_driver.title:
                        break

                    tree = html.fromstring(nav_driver.page_source)
                    links = tree.xpath("//li[contains(@class, 'product')]//a[contains(@class, 'link')]/@href")
                    if not links:
                        links = tree.xpath("//ul[contains(@class, 'products')]//li/a[1]/@href")
                    
                    links = [l for l in links if "product-category" not in l]

                    if not links:
                        break

                    logger.info(f"Processing {len(links)} products...")
                    
                    with ThreadPoolExecutor(max_workers=BIGBEAR_THREADS) as executor:
                        for item_url in links:
                            executor.submit(self.scrape_item_worker, item_url, url, scraper, logger)
                    
                    page += 1
                    if page > 50: break
        finally:
            nav_driver.quit()
            scraper.finalize()