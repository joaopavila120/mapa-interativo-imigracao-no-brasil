from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import PipelineConfig
from src.schema import schema_overlap


LOGGER_NAME = "historical_linkage.preprocessing"


def setup_logging(level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    if logger.handlers:
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _path_is_excluded(path: Path, config: PipelineConfig) -> bool:
    if path.name in config.excluded_file_names:
        return True

    lowered_parts = [part.lower() for part in path.parts]
    return any(
        excluded.lower() == part or excluded.lower() in part
        for excluded in config.excluded_dir_names
        for part in lowered_parts
    )


def discover_csv_files(config: PipelineConfig) -> list[Path]:
    discovered: list[Path] = []
    seen: set[Path] = set()

    for input_dir in config.input_dirs:
        if not input_dir.exists():
            continue

        iterator = input_dir.rglob("*.csv") if config.recursive_input_search else input_dir.glob("*.csv")
        for path in iterator:
            resolved = path.resolve()
            if resolved in seen or _path_is_excluded(resolved, config):
                continue
            seen.add(resolved)
            discovered.append(resolved)

    return sorted(discovered)


def inspect_csv_format(path: Path, config: PipelineConfig) -> tuple[str, str, int] | None:
    best_match: tuple[str, str, int, int] | None = None

    for encoding in config.csv_read_encodings:
        for separator in config.csv_read_separators:
            try:
                preview = pd.read_csv(
                    path,
                    dtype="string",
                    encoding=encoding,
                    sep=separator,
                    nrows=25,
                    on_bad_lines="skip",
                )
            except Exception:
                continue

            candidate = (encoding, separator, schema_overlap(preview.columns), len(preview.columns))
            if best_match is None or candidate[2] > best_match[2] or (
                candidate[2] == best_match[2] and candidate[3] > best_match[3]
            ):
                best_match = candidate

    if best_match and best_match[2] >= config.min_schema_matches:
        return best_match[0], best_match[1], best_match[2]
    return None


def read_csv_file(path: Path, config: PipelineConfig) -> pd.DataFrame:
    format_hint = inspect_csv_format(path, config)
    if format_hint is None:
        raise ValueError(f"Could not match {path.name} to the expected record schema.")

    encoding, separator, _ = format_hint
    return pd.read_csv(
        path,
        dtype="string",
        encoding=encoding,
        sep=separator,
        keep_default_na=False,
        on_bad_lines="skip",
    )


def write_dataframe(df: pd.DataFrame, path: Path) -> None:
    ensure_directory(path.parent)
    df.to_csv(path, index=False)


def write_optional_parquet(df: pd.DataFrame, path: Path, logger: logging.Logger) -> None:
    try:
        ensure_directory(path.parent)
        df.to_parquet(path, index=False)
    except ImportError:
        logger.warning("Skipping parquet export for %s because parquet dependencies are not installed.", path.name)


def write_json(payload: dict[str, Any], path: Path) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
