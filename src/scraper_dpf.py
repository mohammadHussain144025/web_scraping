# src/scraper_dpf.py
import time
from lxml import html
from concurrent.futures import ThreadPoolExecutor

from utils.selenium_factory import create_driver
from utils.base_scraper import BaseScraper
from utils.logger import get_logger
from utils.utils import (
    get_xpath_combined, get_xpath_first, 
    random_sleep, extract_dpf_part, clean_dpf_desc
)
from config.settings import DPF_HEADLESS, DPF_WAIT, DPF_BATCH_SIZE, DPF_THREADS
from utils.constants import oem_list 

SITE_CODE = "DPF"
SITE_NAME = "dpf_parts_direct"
BASE_SITE_URL = "https://www.dpfpartsdirect.com"

MANUFACTURERS = {
    'Redline Emissions Products': 'https://www.dpfpartsdirect.com/collections/redline-dpfs-docs?page={}',
    'Skyline': 'https://www.dpfpartsdirect.com/collections/skyline-dpfs-docs?page={}',
    'Durafit': 'https://www.dpfpartsdirect.com/collections/durafit-dpfs-docs?page={}'
}

class DpfScraper:
    def __init__(self, config):
        self.config = config

    def scrape_item_worker(self, item_url, cat_url, manufacturer, scraper, logger):
        """Worker thread to process individual product pages."""
        driver = create_driver(headless=DPF_HEADLESS, wait_time=DPF_WAIT)
        try:
            logger.info(f"ITEM URL: {item_url}")
            driver.get(item_url)
            tree = html.fromstring(driver.page_source)

            # Extraction logic matching your legacy code
            item_number = get_xpath_first(tree, '//h1/text()')
            description = get_xpath_combined(tree, '//*[@class="rte"]//text() | //*[@class="prod_sku_vend"]//text()')
            price = get_xpath_first(tree, '//*[@id="productPrice"]/@content')

            # --- NEW: Quantity Extraction ---
            # Targets the input element with id="quantity" as seen in your screenshot
            quantity = get_xpath_first(tree, '//input[@id="quantity"]/@value') or "1"

            part_num = extract_dpf_part(description)
            cleaned_desc = clean_dpf_desc(item_number, manufacturer, part_num)

            scraper.add_row({
                "Page URL": cat_url,
                "Item URL": item_url,
                "OEM Name": manufacturer,
                "Item Description": item_number,
                "Item Number": item_number,
                "Price": price,
                "Quantity": quantity, # Added field
                "Currency": "USD",
                "Clean Part Number": part_num,
                "Clean OEM": manufacturer,
                "Clean Part Description": cleaned_desc,
                "Source Name": "DPF Parts Direct",
                "Raw Data": description
            })
            logger.info(f"   SUCCESS -> {item_number} | Price: {price} | Qty: {quantity}")
        except Exception as e:
            logger.error(f"Error scraping {item_url}: {e}")
        finally:
            driver.quit()

    def run(self):
        logger = get_logger(SITE_NAME)
        scraper = BaseScraper(SITE_NAME, DPF_BATCH_SIZE, logger)
        nav_driver = create_driver(headless=DPF_HEADLESS, wait_time=DPF_WAIT)

        logger.info(f"STARTING THREADED SCRAPER: {SITE_NAME}")

        try:
            for manufacturer, url_template in MANUFACTURERS.items():
                page = 1
                while True:
                    url = url_template.format(page)
                    logger.info(f"FETCHING CATEGORY PAGE: {url}")
                    
                    nav_driver.get(url)
                    random_sleep(3, 5)
                    tree = html.fromstring(nav_driver.page_source)

                    # Get product links
                    relative_links = tree.xpath('//div[@class="boost-pfs-filter-product-bottom"]//a/@href')
                    
                    if not relative_links:
                        logger.info(f"No more products for {manufacturer}.")
                        break

                    # Convert to absolute URLs
                    item_urls = [BASE_SITE_URL + link for link in relative_links]
                    logger.info(f"Found {len(item_urls)} products for {manufacturer} on Page {page}")

                    # Multi-threaded product scraping
                    with ThreadPoolExecutor(max_workers=DPF_THREADS) as executor:
                        for item_url in item_urls:
                            executor.submit(self.scrape_item_worker, item_url, url, manufacturer, scraper, logger)
                    
                    page += 1
        finally:
            nav_driver.quit()
            scraper.finalize()
            logger.info(f"FINISHED SCRAPER: {SITE_NAME}")