# src/scraper_filter_service_supply.py
import time
from lxml import html
from utils.selenium_factory import create_driver
from utils.base_scraper import BaseScraper
from utils.logger import get_logger
from utils.utils import (
    get_xpath_combined, get_xpath_first, extract_part_number,
    clean_oem_from_text, match_from_description, random_sleep, remove_punctuation
)
from config.settings import FSS_BATCH_SIZE
from utils.constants import oem_list, c_c_p_all, dis_all 

SITE_CODE = "FSS"
SITE_NAME = "filter_service_supply"

class FilterServiceAndSupplyScraper:
    def __init__(self, config):
        self.start_urls = config[SITE_CODE]["start_urls"]

    def run(self):
        logger = get_logger(SITE_NAME)
        scraper = BaseScraper(SITE_NAME, FSS_BATCH_SIZE, logger)
        driver = create_driver()

        logger.info(f"STARTING SCRAPER: {SITE_NAME}")

        try:
            for start_url in self.start_urls:
                page = 1
                while True:
                    url = start_url.format(page)
                    logger.info(f"FETCHING PAGE: {url}")
                    
                    driver.get(url)
                    random_sleep(3, 5)

                    tree = html.fromstring(driver.page_source)
                    links = tree.xpath('//h3[@class="card-title eq-h"]//a/@href')

                    logger.info(f"PRODUCTS FOUND ON PAGE: {len(links)}")

                    if not links:
                        logger.info("No more products. Stopping category.")
                        break

                    for link in links:
                        logger.info(f"ITEM URL: {link}")
                        driver.get(link)
                        random_sleep(2, 4)
                        tree_3 = html.fromstring(driver.page_source)

                        sku = get_xpath_first(tree_3, '//dd[@itemprop="sku"]/text()')
                        price = get_xpath_first(tree_3, '//meta[@itemprop="price"]/@content')
                        if not price:
                             price = get_xpath_first(tree_3, '//span[@data-product-price-without-tax]/text()')

                        quantity = get_xpath_first(tree_3, '//input[@id="qty[]"]/@value')

                        logger.info(f"   DATA -> SKU: {sku} | PRICE: {price} | QTY: {quantity}")

                        manufacturer = get_xpath_combined(tree_3, '//span[@itemprop="name"]//text()')
                        title = get_xpath_first(tree_3, '//h1[@class="productView-title"]//text()')
                        description = get_xpath_combined(tree_3, '//div[@itemprop="description"]//text()')
                        stock = get_xpath_combined(tree_3, '//div[@class="line-item-details availability"]//dd/text()')

                        engine_model = match_from_description(description, c_c_p_all)
                        displacement = match_from_description(description, dis_all)
                        clean_part = extract_part_number(manufacturer)
                        clean_oem = clean_oem_from_text(manufacturer, oem_list)
                        clean_desc = remove_punctuation(title.replace(displacement, "").replace(clean_part, ""))

                        scraper.add_row({
                            "Page URL": url,
                            "Item URL": link,
                            "OEM Name": manufacturer,
                            "SKU": sku,
                            "Part Description": title,
                            "Price": price,
                            "Currency": "USD",
                            "Quantity": quantity,
                            "Engine Model": engine_model,
                            "Displacement": displacement,
                            "Clean Part Number": clean_part,
                            "Clean OEM": clean_oem,
                            "Clean Part Description": clean_desc,
                            "Stock Availability": stock,
                            "Source Name": "Filter Service And Supply (Skyline)",
                            "Category": start_url.split("/")[-2]
                        })
                    page += 1
        finally:
            scraper.finalize()
            driver.quit()