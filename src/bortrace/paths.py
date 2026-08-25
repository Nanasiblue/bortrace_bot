from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LEGACY_DATA_ROOT = PROJECT_ROOT.parent / "bortrace_data"
LEGACY_TXT_DIR = LEGACY_DATA_ROOT / "txt"
LEGACY_LZH_DIR = LEGACY_DATA_ROOT / "lzh"
OUTPUT_DIR = PROJECT_ROOT / "outputs"
WORK_DIR = PROJECT_ROOT / "work"
