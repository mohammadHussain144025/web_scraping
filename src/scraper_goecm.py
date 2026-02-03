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
    get_xpath_combined, get_xpath_first, get_last_token_if_digit,
    clean_description_modular, random_sleep, match_from_description
)
from config.settings import GOECM_HEADLESS, GOECM_WAIT, GOECM_BATCH_SIZE, GOECM_THREADS
from utils.constants import oem_list, c_c_p_all, dis_all 

SITE_NAME = "goecm"
START_URL_TEMPLATE = "https://goecm.com/collections/all?page={}"

class GoecmScraper:
    def __init__(self, config):
        self.config = config

    def scrape_item_worker(self, item_url, cat_url, scraper, logger):
        # Create driver per thread for speed
        driver = create_driver(headless=GOECM_HEADLESS, wait_time=GOECM_WAIT)
        try:
            logger.info(f"ITEM URL: {item_url}")
            driver.get(item_url)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, "product-single__title")))
            
            tree_3 = html.fromstring(driver.page_source)
            title = get_xpath_first(tree_3, '//h1[@class="h2 product-single__title"]//text()')
            raw_data = get_xpath_combined(tree_3, '//div[@class="table-wrapper"]//text()')
            price = get_xpath_combined(tree_3, '//span[@class="product__price"]//text()')

            found_oem = match_from_description(raw_data, oem_list)
            found_eng = match_from_description(raw_data, c_c_p_all)
            found_dis = match_from_description(raw_data, dis_all)
            part = get_last_token_if_digit(title)
            desc = clean_description_modular(title, found_oem, part)

            scraper.add_row({
                "Page URL": cat_url, "Item URL": item_url, "Item description": title, "Part Number": raw_data,
                "OEM Name": found_oem, "Engine Model": found_eng, "Price": price, "Displacement": found_dis,
                "Active/Analytical (Auxiliary)": "USD", "Clean Part Number": part, "Clean OEM": found_oem,
                "Clean Part Description": desc, "Source Name": "GoECM", "Raw Data": raw_data
            })
            logger.info(f"   DATA -> {part} | {price}")
        except Exception as e:
            logger.error(f"Error at {item_url}: {e}")
        finally:
            driver.quit()

    def run(self):
        logger = get_logger(SITE_NAME)
        scraper = BaseScraper(SITE_NAME, GOECM_BATCH_SIZE, logger)
        nav_driver = create_driver(headless=GOECM_HEADLESS, wait_time=GOECM_WAIT)

        logger.info(f"STARTING FAST SCRAPER: {SITE_NAME}")

        try:
            page = 1
            while True:
                url = START_URL_TEMPLATE.format(page)
                logger.info(f"FETCHING PAGE: {url}")
                nav_driver.get(url)
                random_sleep(2, 3)

                tree = html.fromstring(nav_driver.page_source)
                links = tree.xpath('//a[@class="grid-product__link"]/@href')
                
                if not links: break

                item_urls = [l if l.startswith('http') else f"https://goecm.com{l}" for l in links]

                # Use threads from .env
                with ThreadPoolExecutor(max_workers=GOECM_THREADS) as executor:
                    for item_url in item_urls:
                        executor.submit(self.scrape_item_worker, item_url, url, scraper, logger)
                page += 1
        finally:
            nav_driver.quit()
            scraper.finalize()