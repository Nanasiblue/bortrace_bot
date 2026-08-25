from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

from .paths import LEGACY_DATA_ROOT, PROJECT_ROOT


class _DummyModule(types.ModuleType):
    def __getattr__(self, name):
        dummy = _DummyModule(name)
        setattr(self, name, dummy)
        return dummy

    def __call__(self, *args, **kwargs):
        return self


def _install_dummy_imports() -> None:
    for name in ["lightgbm", "matplotlib", "matplotlib.pyplot", "seaborn"]:
        if name not in sys.modules:
            sys.modules[name] = _DummyModule(name)


def load_legacy_training_module():
    _install_dummy_imports()
    module_path = LEGACY_DATA_ROOT / "train_and_backtest_v4.py"
    spec = importlib.util.spec_from_file_location("legacy_train_and_backtest_v4", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load legacy module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse_legacy_dataset(start_year: int = 2023, end_year: int = 2026):
    module = load_legacy_training_module()
    parser = module.BoatDataParserV4(LEGACY_DATA_ROOT)
    df = parser.parse_all(start_year=start_year, end_year=end_year)
    return module.feature_engineering_v4(df)
