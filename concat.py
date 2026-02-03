import os
import pandas as pd
import glob

# =========================
# CONFIGURATION
# =========================
DATA_DIR = "data"
FINAL_CONCAT_DIR = "final_data" 
RUN_MONTH = "Jan_2026"

# Column names (Case sensitive - double check your CSV headers!)
URL_COL = "Item URL" 
PRICE_COL = "Price"

WEBSITES = [
    # "ag_kits",
    "big_bear_engine", "bostech_auto", "bullet_proof_diesel",
    "dpf_parts_direct", "filter_service_supply", "find_it_parts",
    "goecm", "hd_turbo", "vander_haags"
]

def run_concatenation_process():
    if not os.path.exists(FINAL_CONCAT_DIR):
        os.makedirs(FINAL_CONCAT_DIR)

    summary_stats = []

    for site in WEBSITES:
        folder_name = f"{site}_{RUN_MONTH}"
        folder_path = os.path.join(DATA_DIR, folder_name)

        if not os.path.exists(folder_path):
            continue

        all_files = glob.glob(os.path.join(folder_path, "*.csv"))
        all_files = [f for f in all_files if "final_concat" not in f]

        if not all_files:
            continue

        print(f"Processing: {site}...")

        df_list = []
        for file in all_files:
            try:
                df = pd.read_csv(file, low_memory=False)
                df_list.append(df)
            except Exception as e:
                print(f"  --> Error reading {file}: {e}")

        if df_list:
            # 1. Combine all batches
            combined_df = pd.concat(df_list, ignore_index=True)
            total_raw_rows = len(combined_df)

            # 2. REMOVE DUPLICATES (Keeping the first occurrence)
            if URL_COL in combined_df.columns:
                # This line removes the duplicates from the dataframe
                combined_df.drop_duplicates(subset=[URL_COL], keep='first', inplace=True)
                rows_after_dedup = len(combined_df)
                duplicate_count = total_raw_rows - rows_after_dedup
            else:
                duplicate_count = "N/A"
                rows_after_dedup = total_raw_rows

            # 3. COUNT VALID PRICES (on the deduplicated data)
            valid_price_count = 0
            if PRICE_COL in combined_df.columns:
                valid_prices = combined_df[
                    combined_df[PRICE_COL].notna() & 
                    (combined_df[PRICE_COL].astype(str).str.strip() != "")
                ]
                valid_price_count = len(valid_prices)

            # 4. SAVE TO FINAL FOLDER
            output_filename = f"{site}_final_concat_{RUN_MONTH}.csv"
            output_path = os.path.join(FINAL_CONCAT_DIR, output_filename)
            combined_df.to_csv(output_path, index=False)

            summary_stats.append({
                "Website": site,
                "Raw Total": total_raw_rows,
                "Duplicates Removed": duplicate_count,
                "Final Unique Items": rows_after_dedup,
                "With Price": valid_price_count
            })

    # --- FINAL REPORT ---
    if summary_stats:
        print("\n" + "="*85)
        print(f"{'WEBSITE':<22} | {'RAW':<7} | {'DUPS':<7} | {'UNIQUE':<8} | {'W/ PRICE':<8}")
        print("-" * 85)
        for stat in summary_stats:
            print(f"{stat['Website']:<22} | {stat['Raw Total']:<7} | {stat['Duplicates Removed']:<7} | "
                  f"{stat['Final Unique Items']:<8} | {stat['With Price']:<8}")
        print("="*85)
        print(f"Success! All deduplicated files are in: {FINAL_CONCAT_DIR}")

if __name__ == "__main__":
    run_concatenation_process()