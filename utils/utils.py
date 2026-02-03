# common/utils.py

import re
import string
import time
import random

def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def remove_punctuation(text: str) -> str:
    if not text:
        return ""
    chars = re.escape(string.punctuation)
    return re.sub(f"[{chars}]", "", text).strip()

def get_xpath_combined(tree, xpath: str) -> str:
    try:
        values = tree.xpath(xpath)
        if not values:
            return ""
        return normalize_text(" ".join([str(v) for v in values if v]))
    except Exception:
        return ""

def get_xpath_first(tree, xpath: str) -> str:
    try:
        values = tree.xpath(xpath)
        return normalize_text(str(values[0])) if values else ""
    except Exception:
        return ""

def extract_part_number(text: str) -> str:
    if not text:
        return ""
    try:
        tokens = [p for p in text.split() if any(ch.isdigit() for ch in p) and len(p) > 4]
        return tokens[0] if tokens else ""
    except Exception:
        return ""

def clean_oem_from_text(text: str, oem_list: list) -> str:
    if not text:
        return ""
    try:
        text_lower = text.lower()
        for oem in oem_list:
            oem_lower = oem.lower()

            if oem_lower in text_lower and "volvo/mack" in text_lower:
                return "Volvo/Mack"

            if oem == "Mercedes" and oem_lower in text_lower:
                return "Mercedes-Benz"

            if oem_lower in text_lower:
                return oem

        return ""
    except Exception:
        return ""

def match_from_description(description: str, candidates: list) -> str:
    if not description:
        return ""
    for c in candidates:
        if str(c) in description:
            return str(c)
    return ""


def get_last_token_if_digit(text: str) -> str:
    """Old get_part logic: extracts last word if it contains a digit."""
    if not text: return ""
    tokens = text.strip().split(" ")
    last_token = tokens[-1]
    if any(char.isdigit() for char in last_token):
        return last_token
    return ""

def clean_description_modular(desc, oem, part):
    """Old clean_desc logic: removes oem/part and punctuation."""
    if not desc: return ""
    text = desc.replace(str(oem), "").replace(str(part), "")
    chars = re.escape(string.punctuation)
    return re.sub(f'[{chars}]', '', text).strip()

def random_sleep(min_sec=2.5, max_sec=4.5):
    time.sleep(random.uniform(min_sec, max_sec))

#dpf
def extract_dpf_part(description: str) -> str:
    """Legacy get_part logic: extracts text inside parentheses."""
    if not description: return ""
    try:
        text = description.split("(")[-1].split(")")[0].strip()
        return text if len(text) < 40 else ""
    except:
        return ""

def clean_dpf_desc(item_num, manufacturer, part_num):
    """Legacy clean_desc logic: Removes manufacturer and part number from title."""
    if not item_num: return ""
    text = str(item_num).replace(str(manufacturer), "").replace(str(part_num), "")
    chars = re.escape(string.punctuation)
    return re.sub(f'[{chars}]', '', text).strip()

#findit parts
def get_random_user_agent():
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    ]
    return random.choice(agents)


#vander haags
def extract_vander_part(description: str) -> str:
    """Legacy get_part logic: split by '='."""
    if not description: return ""
    try:
        return description.split("=")[-1].strip()
    except:
        return ""

def extract_vander_oem(description: str) -> str:
    """Legacy clean_oem logic: split and take second word."""
    if not description: return ""
    try:
        return description.split()[1].strip()
    except:
        return ""