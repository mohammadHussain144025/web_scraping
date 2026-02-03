# src/scraper_bostech.py
import time
from lxml import html
from concurrent.futures import ThreadPoolExecutor
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException

from utils.selenium_factory import create_driver
from utils.base_scraper import BaseScraper
from utils.logger import get_logger
from utils.utils import (
    get_xpath_combined, get_xpath_first, 
    random_sleep, match_from_description
)
from config.settings import BOSTECH_HEADLESS, BOSTECH_WAIT, BOSTECH_BATCH_SIZE, BOSTECH_THREADS
from utils.constants import oem_list, c_c_p_all, dis_all 

SITE_CODE = "BOSTECH"
SITE_NAME = "bostech_auto"
BASE_URL = "https://bostechauto.com"

class BostechScraper:
    def __init__(self, config):
        self.config = config
        # Fallback categories in case discovery fails
        self.fallback_categories = [
            'https://bostechauto.com/product/oil-systems/hpop-high-pressure-oil-pump/',
            'https://bostechauto.com/product/emission-systems/egr-cooler/',
            'https://bostechauto.com/product/fuel-air-system/fuel-injector-1/',
            'https://bostechauto.com/product/coolant/water-pumps/',
            'https://bostechauto.com/product/fuel-air-system/fuel-harness-connectors/',
            'https://bostechauto.com/product/sensors/'
        ]

    def close_modals(self, driver):
        """Attempts to close common e-commerce pop-ups."""
        try:
            # Common close button selectors for BigCommerce/Shopify popups
            close_selectors = [
                "//button[@aria-label='Close dialog']",
                "//button[contains(@class, 'modal-close')]",
                "//a[contains(@class, 'close-window')]",
                "//span[contains(text(), '×')]"
            ]
            for selector in close_selectors:
                buttons = driver.find_elements(By.XPATH, selector)
                for btn in buttons:
                    if btn.is_displayed():
                        btn.click()
                        time.sleep(1)
        except:
            pass

    def get_category_urls(self, logger):
        """Discovers category links from the home page with broad XPaths."""
        logger.info("Attempting to discover categories from home page...")
        driver = create_driver(headless=BOSTECH_HEADLESS, wait_time=BOSTECH_WAIT)
        try:
            driver.get(BASE_URL)
            self.close_modals(driver)
            
            # Wait for any category link to appear instead of a specific class
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'section-content')]//a"))
                )
            except TimeoutException:
                logger.warning("Home page category section timed out. Using fallback list.")
                return self.fallback_categories

            tree = html.fromstring(driver.page_source)
            # Find links inside the category section or any link containing '/product/'
            links = tree.xpath('//div[contains(@class, "section-content")]//a/@href')
            
            if not links:
                return self.fallback_categories

            # Resolve relative paths
            full_links = list(set([BASE_URL + l if l.startswith('/') else l for l in links if '/product/' in l]))
            logger.info(f"Discovered {len(full_links)} categories.")
            return full_links
        finally:
            driver.quit()

    def scrape_item_worker(self, item_url, cat_url, scraper, logger):
        """Worker thread for individual product pages."""
        driver = create_driver(headless=BOSTECH_HEADLESS, wait_time=BOSTECH_WAIT)
        try:
            logger.info(f"ITEM URL: {item_url}")
            driver.get(item_url)
            self.close_modals(driver)
            
            # Wait for any core product element
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".productView-title, h1")))
            
            tree = html.fromstring(driver.page_source)
            
            title = get_xpath_first(tree, '//h1[contains(@class, "productView-title")]//text()')
            sku = get_xpath_first(tree, '//dd[contains(@class, "productView-info-value")]//text()')
            price = get_xpath_first(tree, '//span[contains(@class, "price--withoutTax")]//text()')
            
            # Extraction of Quantity from value attribute
            quantity = get_xpath_first(tree, '//input[@id="qty[]"]/@value') or "1"
            
            raw_data = get_xpath_combined(tree, '//div[contains(@class, "tab-content") and contains(@class, "is-active")]//text()')

            found_oem = match_from_description(title + " " + raw_data, oem_list)
            found_eng = match_from_description(raw_data, c_c_p_all)

            scraper.add_row({
                "Page URL": cat_url,
                "Item URL": item_url,
                "Item description": title,
                "SKU": sku,
                "Price": price,
                "Quantity": quantity,
                "OEM Name": found_oem,
                "Engine Model": found_eng,
                "Currency": "USD",
                "Source Name": "Bostech Auto",
                "Raw Data": raw_data
            })
            logger.info(f"   SUCCESS -> SKU: {sku} | Qty: {quantity}")
        except Exception as e:
            logger.error(f"Error scraping {item_url}: {str(e)[:100]}")
        finally:
            driver.quit()

    def run(self):
        logger = get_logger(SITE_NAME)
        scraper = BaseScraper(SITE_NAME, BOSTECH_BATCH_SIZE, logger)
        
        categories = self.get_category_urls(logger)
        nav_driver = create_driver(headless=BOSTECH_HEADLESS, wait_time=BOSTECH_WAIT)

        logger.info(f"STARTING MULTI-THREADED SCRAPER: {SITE_NAME}")

        try:
            for cat_url in categories:
                logger.info(f"NAVIGATING TO CATEGORY: {cat_url}")
                nav_driver.get(cat_url)
                self.close_modals(nav_driver)
                random_sleep(3, 5)

                # Set items per page to 12 if dropdown exists
                try:
                    select_el = nav_driver.find_element(By.XPATH, '//select[@class="select_limit"]')
                    dropdown = Select(select_el)
                    dropdown.select_by_value("12")
                    random_sleep(2, 3)
                except:
                    pass

                while True:
                    # Wait for items to load
                    try:
                        WebDriverWait(nav_driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "APAcol")]//a'))
                        )
                    except TimeoutException:
                        logger.warning(f"No products found on view for category: {cat_url}")
                        break
                    
                    tree = html.fromstring(nav_driver.page_source)
                    # Extract item links (matching 'see-details' or 'details')
                    links = tree.xpath('//div[contains(@class, "APAcol")]//a[contains(@class, "details")]/@href')
                    
                    if not links:
                        break

                    logger.info(f"Page found: Processing {len(links)} products in parallel...")
                    
                    full_urls = [BASE_URL + l if l.startswith('/') else l for l in links]

                    with ThreadPoolExecutor(max_workers=BOSTECH_THREADS) as executor:
                        for item_url in full_urls:
                            executor.submit(self.scrape_item_worker, item_url, cat_url, scraper, logger)

                    # AJAX Pagination
                    try:
                        next_btn = nav_driver.find_element(By.XPATH, '//a[contains(@class, "nextPage")]')
                        # Check if disabled
                        if "APAbtn-disabled" in next_btn.get_attribute("class"):
                            break
                        nav_driver.execute_script("arguments[0].click();", next_btn)
                        random_sleep(4, 6) # Give extra time for AJAX load
                    except:
                        break 
        finally:
            nav_driver.quit()
            scraper.finalize()