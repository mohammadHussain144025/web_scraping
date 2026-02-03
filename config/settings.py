import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# CORE PATHS - Strictly from .env
BASE_DIR = Path(os.getenv("BASE_DIR", ".")).resolve()
RESOURCES_DIR = BASE_DIR / os.getenv("RESOURCES_DIR", ".")
DATA_DIR = BASE_DIR / os.getenv("DATA_DIR", "data")
LOG_DIR = BASE_DIR / os.getenv("LOG_DIR", "logs")

# Ensure dirs exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# RUN CONFIG
RUN_MONTH = os.getenv("RUN_MONTH", "Jan_2026")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 3))

# FSS CONFIG (Using your exact .env keys)
FSS_HEADLESS = os.getenv("FILTER_SERVICE_SUPPLY_SELENIUM_HEADLESS", "true").lower() == "true"
FSS_WAIT = int(os.getenv("FILTER_SERVICE_SUPPLY_SELENIUM_IMPLICIT_WAIT", 30))
FSS_BATCH_SIZE = int(os.getenv("FSS_BATCH_SIZE", 12))


# GOECM CONFIG (Using your exact .env keys)
GOECM_HEADLESS = os.getenv("GOECM_SELENIUM_HEADLESS", "false").lower() == "true"
GOECM_WAIT = int(os.getenv("GOECM_SELENIUM_IMPLICIT_WAIT", 30))
GOECM_BATCH_SIZE = int(os.getenv("GOECM_BATCH_SIZE", 12))
GOECM_THREADS = int(os.getenv("GOECM_THREADS", 4))


# BIGBEAR CONFIG
BIGBEAR_HEADLESS = os.getenv("BIGBEAR_HEADLESS", "true").lower() == "true"
BIGBEAR_WAIT = int(os.getenv("BIGBEAR_WAIT", 30))
BIGBEAR_BATCH_SIZE = int(os.getenv("BIGBEAR_BATCH_SIZE", 12))
BIGBEAR_THREADS = int(os.getenv("BIGBEAR_THREADS", 4))


# DPF PARTS DIRECT CONFIG
DPF_HEADLESS = os.getenv("DPF_HEADLESS", "true").lower() == "true"
DPF_WAIT = int(os.getenv("DPF_WAIT", 30))
DPF_BATCH_SIZE = int(os.getenv("DPF_BATCH_SIZE", 12))
DPF_THREADS = int(os.getenv("DPF_THREADS", 4))


# BOSTECH CONFIG
BOSTECH_HEADLESS = os.getenv("BOSTECH_HEADLESS", "true").lower() == "true"
BOSTECH_WAIT = int(os.getenv("BOSTECH_WAIT", 30))
BOSTECH_BATCH_SIZE = int(os.getenv("BOSTECH_BATCH_SIZE", 12))
BOSTECH_THREADS = int(os.getenv("BOSTECH_THREADS", 4))


# FINDITPARTS CONFIG
FINDIT_HEADLESS = os.getenv("FINDIT_HEADLESS", "true").lower() == "true"
FINDIT_WAIT = int(os.getenv("FINDIT_WAIT", 30))
FINDIT_BATCH_SIZE = int(os.getenv("FINDIT_BATCH_SIZE", 100))
FINDIT_THREADS = int(os.getenv("FINDIT_THREADS", 5))
FINDIT_INPUT_FILE = os.getenv("FINDIT_INPUT_FILE", "Find it Parts.xlsx")


# HD TURBO CONFIG
HDTURBO_HEADLESS = os.getenv("HDTURBO_HEADLESS", "true").lower() == "true"
HDTURBO_WAIT = int(os.getenv("HDTURBO_WAIT", 30))
HDTURBO_BATCH_SIZE = int(os.getenv("HDTURBO_BATCH_SIZE", 12))
HDTURBO_THREADS = int(os.getenv("HDTURBO_THREADS", 4))



# AG KITS CONFIG
AGKITS_HEADLESS = os.getenv("AGKITS_HEADLESS", "true").lower() == "true"
AGKITS_WAIT = int(os.getenv("AGKITS_WAIT", 30))
AGKITS_BATCH_SIZE = int(os.getenv("AGKITS_BATCH_SIZE", 12))
AGKITS_THREADS = int(os.getenv("AGKITS_THREADS", 4))



# VANDER HAAGS CONFIG
VANDERHAAGS_HEADLESS = os.getenv("VANDERHAAGS_HEADLESS", "true").lower() == "true"
VANDERHAAGS_WAIT = int(os.getenv("VANDERHAAGS_WAIT", 30))
VANDERHAAGS_BATCH_SIZE = int(os.getenv("VANDERHAAGS_BATCH_SIZE", 12))
VANDERHAAGS_THREADS = int(os.getenv("VANDERHAAGS_THREADS", 5))


# BULLETPROOF CONFIG
BULLETPROOF_HEADLESS = os.getenv("BULLETPROOF_HEADLESS", "true").lower() == "true"
BULLETPROOF_WAIT = int(os.getenv("BULLETPROOF_WAIT", 30))
BULLETPROOF_BATCH_SIZE = int(os.getenv("BULLETPROOF_BATCH_SIZE", 12))
BULLETPROOF_THREADS = int(os.getenv("BULLETPROOF_THREADS", 4))