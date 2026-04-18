from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.config import PipelineConfig
from src.io_utils import (
    discover_csv_files,
    ensure_directory,
    read_csv_file,
    setup_logging,
    write_dataframe,
    write_json,
    write_optional_parquet,
)
from src.preprocessing import build_clean_records, build_combined_records, build_quality_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize historical immigrant record CSVs for linkage.")
    parser.add_argument(
        "--input-dir",
        action="append",
        dest="input_dirs",
        help="Directory containing input CSV files. Repeat the flag to add multiple folders.",
    )
    parser.add_argument("--combined-output", dest="combined_output")
    parser.add_argument("--clean-output", dest="clean_output")
    parser.add_argument("--quality-report", dest="quality_report")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--write-parquet", action="store_true")
    return parser.parse_args()


def build_config(args: argparse.Namespace) -> PipelineConfig:
    config = PipelineConfig(
        input_dirs=tuple(Path(path) for path in args.input_dirs) if args.input_dirs else None,
        log_level=args.log_level,
        write_parquet=args.write_parquet,
    )

    if args.combined_output:
        config.interim_dir = Path(args.combined_output).resolve().parent
        config.combined_filename = Path(args.combined_output).name
    if args.clean_output:
        config.processed_dir = Path(args.clean_output).resolve().parent
        config.clean_filename = Path(args.clean_output).name
    if args.quality_report:
        config.quality_report_filename = Path(args.quality_report).name
        config.processed_dir = Path(args.quality_report).resolve().parent

    return config


def main() -> int:
    args = parse_args()
    config = build_config(args)
    logger = setup_logging(config.log_level)

    ensure_directory(config.interim_dir)
    ensure_directory(config.processed_dir)

    input_files = discover_csv_files(config)
    if not input_files:
        logger.error("No compatible CSV files were found in: %s", ", ".join(str(path) for path in config.input_dirs))
        return 1

    frames: list[pd.DataFrame] = []
    inventory: list[dict[str, object]] = []

    for path in input_files:
        try:
            frame = read_csv_file(path, config)
        except ValueError as exc:
            logger.info("Skipping %s: %s", path.name, exc)
            inventory.append({"file": str(path), "status": "skipped", "reason": str(exc)})
            continue
        except Exception as exc:
            logger.exception("Failed to read %s", path)
            inventory.append({"file": str(path), "status": "error", "reason": str(exc)})
            continue

        try:
            relative_path = path.relative_to(config.project_root)
            input_file_value = str(relative_path)
        except ValueError:
            input_file_value = str(path)

        frame["input_file"] = input_file_value
        frame["input_stem"] = path.stem
        frame["source_row_number"] = pd.Series(range(1, len(frame) + 1), dtype="Int64")
        frames.append(frame)
        inventory.append({"file": str(path), "status": "loaded", "rows": int(len(frame))})
        logger.info("Loaded %s rows from %s.", len(frame), path.name)

    if not frames:
        logger.error("CSV files were discovered, but none matched the expected record schema.")
        return 1

    combined_raw = pd.concat(frames, ignore_index=True, sort=False)
    combined_df = build_combined_records(combined_raw, config)
    clean_df = build_clean_records(combined_raw, config, logger=logger)
    quality_report = build_quality_report(combined_df, clean_df, inventory)

    write_dataframe(combined_df, config.combined_output_path)
    write_dataframe(clean_df, config.clean_output_path)
    write_json(quality_report, config.quality_report_path)

    if config.write_parquet:
        write_optional_parquet(combined_df, config.combined_parquet_path, logger)
        write_optional_parquet(clean_df, config.clean_parquet_path, logger)

    logger.info("Wrote combined dataset to %s", config.combined_output_path)
    logger.info("Wrote clean dataset to %s", config.clean_output_path)
    logger.info("Wrote quality report to %s", config.quality_report_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
