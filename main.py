# main.py

import sys
import json
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

from config.settings import MAX_WORKERS
from src.scraper_filter_service_supply import FilterServiceAndSupplyScraper
from src.scraper_goecm import GoecmScraper # Import the new scraper
from src.scraper_bigbear import BigBearScraper
from src.scraper_dpf import DpfScraper
from src.scraper_bostech import BostechScraper
from src.scraper_finditparts import FindItPartsScraper
from src.scraper_hdturbo import HDTurboScraper
from src.scraper_agkits import AgKitsScraper
from src.scraper_vanderhaags import VanderHaagsScraper
from src.scraper_bulletproof import BulletproofScraper

SITE_REGISTRY = {
     "FSS": FilterServiceAndSupplyScraper,
     "GOECM": GoecmScraper,
     "BIGBEAR": BigBearScraper,
     "DPF": DpfScraper, 
     "BOSTECH": BostechScraper, 
     "FINDIT": FindItPartsScraper,
     "HDTURBO": HDTurboScraper,
     "AGKITS": AgKitsScraper,
     "VANDERHAAGS": VanderHaagsScraper,
     "BULLETPROOF": BulletproofScraper,
}

def load_site_config():
    config_path = Path("config") / "config.json"

    if not config_path.exists():
        raise FileNotFoundError(f"Missing config file: {config_path.resolve()}")

    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)

def run_scraper(scraper_cls, config):
    scraper = scraper_cls(config)
    scraper.run()

def main():
    config = load_site_config()

    cli_sites = [arg.upper() for arg in sys.argv[1:]]

    if not cli_sites:
        print("❌ Usage: python main.py FSS [CAT BPD]")
        return

    invalid = [s for s in cli_sites if s not in SITE_REGISTRY]
    if invalid:
        raise ValueError(f"Invalid site code(s): {invalid}")

    scrapers_to_run = [SITE_REGISTRY[code] for code in cli_sites]

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(run_scraper, scraper_cls, config)
            for scraper_cls in scrapers_to_run
        ]

        for f in futures:
            f.result()

if __name__ == "__main__":
    main()
