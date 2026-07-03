# =====================================================================
# Power BI Python bridge — Cohesity Physical PG Inventory
#
# Use this script in Power BI Desktop:
# Home > Get data > Python script
#
# What it does:
# 1. Runs Invoke-PhysicalPGInventoryHeadless.ps1
# 2. Reads Physical_PG_Summary_Latest.csv
# 3. Reads Physical_PG_Object_Detail_Latest.csv
# 4. Exposes two pandas DataFrames for Power BI Navigator
#
# Prerequisites:
# - Python installed locally
# - pandas installed
# - Power BI Desktop configured to use the same Python installation
# - powershell.exe available
# =====================================================================

import subprocess
from pathlib import Path

import pandas as pd

BASE_DIR = Path(r"X:\PowerShell\Cohesity_API_Scripts\inventory")
HEADLESS_SCRIPT = BASE_DIR / "Invoke-PhysicalPGInventoryHeadless.ps1"
SUMMARY_CSV = BASE_DIR / "Physical_PG_Summary_Latest.csv"
DETAIL_CSV = BASE_DIR / "Physical_PG_Object_Detail_Latest.csv"

CLUSTER_SELECTION = "0"  # 0 = ALL clusters. Change to a cluster menu number only if required.
TIMEOUT_SECONDS = 3600

if not HEADLESS_SCRIPT.exists():
    raise FileNotFoundError(f"Headless PowerShell wrapper not found: {HEADLESS_SCRIPT}")

cmd = [
    "powershell.exe",
    "-NoProfile",
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    str(HEADLESS_SCRIPT),
    "-ClusterSelection",
    CLUSTER_SELECTION,
]

completed = subprocess.run(
    cmd,
    capture_output=True,
    text=True,
    timeout=TIMEOUT_SECONDS,
)

if completed.returncode != 0:
    raise RuntimeError(
        "PowerShell inventory collector failed.\n"
        f"Command: {' '.join(cmd)}\n"
        f"Return code: {completed.returncode}\n"
        f"STDOUT:\n{completed.stdout}\n"
        f"STDERR:\n{completed.stderr}"
    )

if not SUMMARY_CSV.exists():
    raise FileNotFoundError(f"Summary CSV not found after collector run: {SUMMARY_CSV}")

if not DETAIL_CSV.exists():
    raise FileNotFoundError(f"Detail CSV not found after collector run: {DETAIL_CSV}")

# Power BI Navigator will show these two DataFrames.
Physical_PG_Summary = pd.read_csv(SUMMARY_CSV, dtype=str).fillna("")
Physical_PG_Object_Detail = pd.read_csv(DETAIL_CSV, dtype=str).fillna("")
