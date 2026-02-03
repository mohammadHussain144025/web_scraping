from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

from utils.utils import get_random_user_agent

def create_driver(headless: bool, wait_time: int):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    
    options.add_argument(f"user-agent={get_random_user_agent()}")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    service = Service()
    driver = webdriver.Chrome(service=service, options=options)
    driver.implicitly_wait(wait_time)
    return driver