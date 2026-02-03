# src/scraper_bulletproof.py
import time
import numpy as np
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
from config.settings import (
    BULLETPROOF_HEADLESS, BULLETPROOF_WAIT, 
    BULLETPROOF_BATCH_SIZE, BULLETPROOF_THREADS
)
from utils.constants import oem_list, c_c_p_all, dis_all 

SITE_CODE = "BULLETPROOF"
SITE_NAME = "bullet_proof_diesel"
BASE_URL = "https://bulletproofdiesel.com"
COLLECTION_URL = "https://bulletproofdiesel.com/collections/all"

class BulletproofScraper:
    def __init__(self, config):
        self.config = config

    def get_total_pages(self, logger):
        """Logic to calculate total pages from the category count element."""
        driver = create_driver(headless=BULLETPROOF_HEADLESS, wait_time=BULLETPROOF_WAIT)
        try:
            driver.get(COLLECTION_URL)
            xpath = '//p[@class="boost-pfs-filter-total-product collection__products-count text--small hidden-desk"]'
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, xpath)))
            
            fill_text = driver.find_element(By.XPATH, xpath).text
            # Logic: split and take index -2 as per your snippet
            fill_num = int(fill_text.split()[-2].replace(',', ''))
            
            # Logic: num_of_pages(fill_num, 24)
            pages = (fill_num // 24) + (1 if fill_num % 24 > 0 else 0)
            
            logger.info(f"Total Products: {fill_num} | Calculated Pages: {pages}")
            return pages
        except Exception as e:
            logger.error(f"Failed to calculate total pages: {e}. Defaulting to 1.")
            return 1
        finally:
            driver.quit()

    def scrape_item_worker(self, item_url, cat_url, scraper, logger):
        """Worker thread for individual product scraping following legacy extraction logic."""
        driver = create_driver(headless=BULLETPROOF_HEADLESS, wait_time=BULLETPROOF_WAIT)
        try:
            logger.info(f"ITEM URL: {item_url}")
            driver.get(item_url)
            
            # Wait for content meta
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, "product-meta")))
            
            tree = html.fromstring(driver.page_source)

            # --- Extraction (Strictly following your provided logic) ---
            title = get_xpath_first(tree, '//h1[@class="product-meta__title heading h1"]//text()')
            sku = get_xpath_first(tree, '//span[@class="product-meta__sku-number"]//text()')
            price = get_xpath_combined(tree, '//div[@class="price-list"]//text()')
            raw_desc = get_xpath_combined(tree, '//div[@class = "product-block-list__item product-block-list__item--content"]//text()')
            
            # --- Added Quantity (from previous screenshot request) ---
            quantity = get_xpath_first(tree, '//input[@name="quantity"]/@value') or "1"

            # Engine Model & Displacement Logic
            emo = ""
            for mo in c_c_p_all:
                if str(mo) in title:
                    emo = str(mo)
                    break
            
            disp = ""
            for dis in dis_all:
                if str(dis) in title:
                    disp = str(dis)
                    break

            scraper.add_row({
                "Page URL": cat_url,
                "Item URL": item_url,
                "Item description": title,
                "SKU": sku,
                "OEM Name": 'Bullet Proof Diesel',
                "Engine Model": emo,
                "Price": price,
                "Certification": "",
                "Horse Power": "",
                "Displacement": disp,
                "Warranty": "",
                "Stock Availability": "",
                "Lead Time": "",
                "Active/Analytical (Auxiliary)": "",
                "Clean Part Description": "", # Placeholder as per legacy row
                "Clean OEM": "",             # Placeholder as per legacy row
                "Clean Part Number": "",      # Placeholder as per legacy row
                "Raw Description": raw_desc,
                "Source Name": 'Bullet Proof Diesel',
                "Currency": "USD",
                "Quantity": quantity
            })
            logger.info(f"   SUCCESS -> SKU: {sku} | Qty: {quantity} | Price: {price[:15]}")
        except Exception as e:
            logger.error(f"Error scraping product {item_url}: {str(e)[:100]}")
        finally:
            driver.quit()

    def run(self):
        logger = get_logger(SITE_NAME)
        scraper = BaseScraper(SITE_NAME, BULLETPROOF_BATCH_SIZE, logger)
        
        pages = self.get_total_pages(logger)
        nav_driver = create_driver(headless=BULLETPROOF_HEADLESS, wait_time=BULLETPROOF_WAIT)

        logger.info(f"STARTING BULLETPROOF SCRAPER (Threads: {BULLETPROOF_THREADS})")

        try:
            # Iterating through all pages
            for page in range(1, pages + 1):
                url = f"{COLLECTION_URL}?page={page}"
                logger.info(f"FETCHING PAGE: {url}")
                
                nav_driver.get(url)
                random_sleep(3, 4)

                tree = html.fromstring(nav_driver.page_source)
                # Find product links using legacy XPath logic
                links = tree.xpath('//div[@class="boost-pfs-filter-products product-list product-list--collection"]//a[@class="product-item__title text--strong link"]/@href')

                if not links:
                    logger.info(f"No products found on page {page}. Ending.")
                    break

                logger.info(f"Page {page}: {len(links)} products found. Processing in parallel...")
                
                full_item_urls = [BASE_URL + l if not l.startswith('http') else l for l in links]

                with ThreadPoolExecutor(max_workers=BULLETPROOF_THREADS) as executor:
                    for item_url in full_item_urls:
                        executor.submit(self.scrape_item_worker, item_url, url, scraper, logger)
                
        finally:
            nav_driver.quit()
            scraper.finalize()