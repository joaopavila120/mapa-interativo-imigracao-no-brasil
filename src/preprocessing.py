from __future__ import annotations

import json
import logging
import re
import unicodedata
from datetime import datetime
from typing import Any

import pandas as pd

from src.config import PipelineConfig
from src.schema import BASE_COLUMNS, FINAL_COLUMNS, align_to_base_schema, reorder_columns

MULTISPACE_RE = re.compile(r"\s+")
YEAR_RE = re.compile(r"\b(17|18|19|20)\d{2}\b")
CHILDREN_COUNT_RE = re.compile(r"\b(\d+)\s+FILH")
MONTH_MARKER_RE = re.compile(r"\b(MES|MESES|MONTH|MONTHS)\b")
DAY_MARKER_RE = re.compile(r"\b(DIA|DIAS|DAY|DAYS)\b")

PT_MONTHS = {
    "JAN": "JAN",
    "JANEIRO": "JAN",
    "FEV": "FEB",
    "FEVEREIRO": "FEB",
    "MAR": "MAR",
    "MARCO": "MAR",
    "ABR": "APR",
    "ABRIL": "APR",
    "MAI": "MAY",
    "MAIO": "MAY",
    "JUN": "JUN",
    "JUNHO": "JUN",
    "JUL": "JUL",
    "JULHO": "JUL",
    "AGO": "AUG",
    "AGOSTO": "AUG",
    "SET": "SEP",
    "SETEMBRO": "SEP",
    "OUT": "OCT",
    "OUTUBRO": "OCT",
    "NOV": "NOV",
    "NOVEMBRO": "NOV",
    "DEZ": "DEC",
    "DEZEMBRO": "DEC",
}


def strip_accents(value: str) -> str:
    return "".join(
        character
        for character in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(character)
    )


def normalize_whitespace(value: str) -> str:
    return MULTISPACE_RE.sub(" ", value).strip()


def normalize_token(value: Any) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return normalize_whitespace(strip_accents(str(value)).upper())


def is_missing(value: Any, null_tokens: set[str]) -> bool:
    normalized = normalize_token(value)
    return not normalized or normalized in null_tokens


def clean_scalar(value: Any, null_tokens: set[str]) -> Any:
    if is_missing(value, null_tokens):
        return pd.NA
    return normalize_whitespace(str(value))


def coalesce(*values: Any) -> Any:
    for value in values:
        if value is None or pd.isna(value):
            continue
        text = str(value).strip()
        if text:
            return text
    return pd.NA


def normalize_text(
    value: Any,
    *,
    keep_digits: bool = True,
    aliases: dict[str, str] | None = None,
    strip_parentheses: bool = False,
) -> Any:
    if value is None or pd.isna(value):
        return pd.NA

    text = normalize_whitespace(str(value))
    if not text:
        return pd.NA

    if strip_parentheses:
        text = re.sub(r"\([^)]*\)", " ", text)

    text = strip_accents(text).upper()
    pattern = r"[^A-Z0-9\s]" if keep_digits else r"[^A-Z\s]"
    text = re.sub(pattern, " ", text)
    text = normalize_whitespace(text)
    if not text:
        return pd.NA

    if aliases:
        return aliases.get(text, text)
    return text


def normalize_name(value: Any) -> Any:
    return normalize_text(value, keep_digits=False)


def normalize_place(value: Any, aliases: dict[str, str] | None = None) -> Any:
    return normalize_text(value, keep_digits=False, aliases=aliases)


def normalize_ship_name(value: Any, aliases: dict[str, str] | None = None) -> Any:
    return normalize_text(value, aliases=aliases, strip_parentheses=True)


def normalize_sex(value: Any) -> Any:
    normalized = normalize_text(value, keep_digits=False)
    if pd.isna(normalized):
        return pd.NA

    mapping = {
        "M": "M",
        "MASC": "M",
        "MASCULINO": "M",
        "MALE": "M",
        "HOMEM": "M",
        "F": "F",
        "FEM": "F",
        "FEMININO": "F",
        "FEMALE": "F",
        "MULHER": "F",
    }
    return mapping.get(normalized, pd.NA)


def parse_integer(value: Any, *, minimum: int | None = None, maximum: int | None = None) -> Any:
    if value is None or pd.isna(value):
        return pd.NA

    text = normalize_whitespace(str(value))
    match = re.search(r"\d{1,4}", text)
    if not match:
        return pd.NA

    parsed = int(match.group(0))
    if minimum is not None and parsed < minimum:
        return pd.NA
    if maximum is not None and parsed > maximum:
        return pd.NA
    return parsed


def parse_age(value: Any) -> Any:
    if value is None or pd.isna(value):
        return pd.NA

    normalized = normalize_token(value)
    if not normalized:
        return pd.NA
    if MONTH_MARKER_RE.search(normalized) or DAY_MARKER_RE.search(normalized):
        return 0
    return parse_integer(value, minimum=0, maximum=120)


def extract_year(value: Any) -> Any:
    if value is None or pd.isna(value):
        return pd.NA

    text = normalize_whitespace(str(value))
    match = YEAR_RE.search(text)
    if not match:
        return pd.NA
    return int(match.group(0))


def _translate_month_names(text: str) -> str:
    translated = text
    for source, target in PT_MONTHS.items():
        translated = re.sub(rf"\b{source}\b", target, translated, flags=re.IGNORECASE)
    return translated


def normalize_date(value: Any) -> tuple[Any, Any, Any]:
    if value is None or pd.isna(value):
        return pd.NA, pd.NA, pd.NA

    original = normalize_whitespace(str(value))
    if not original:
        return pd.NA, pd.NA, pd.NA

    translated = _translate_month_names(original)
    translated = translated.replace(".", "/").replace("-", "/")

    if re.fullmatch(r"\d{4}", translated):
        year = int(translated)
        return translated, "year", year

    known_formats = ("%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d", "%d %b %Y", "%d %B %Y")
    for fmt in known_formats:
        try:
            parsed = datetime.strptime(translated, fmt)
            return parsed.date().isoformat(), "day", parsed.year
        except ValueError:
            continue

    parsed = pd.to_datetime(translated, dayfirst=True, errors="coerce")
    if not pd.isna(parsed):
        return parsed.date().isoformat(), "day", int(parsed.year)

    year = extract_year(translated)
    if pd.isna(year):
        return pd.NA, pd.NA, pd.NA
    return str(year), "year", int(year)


def split_name_components(value: Any) -> tuple[Any, Any]:
    if value is None or pd.isna(value):
        return pd.NA, pd.NA

    text = normalize_whitespace(str(value))
    if not text:
        return pd.NA, pd.NA

    if "," in text:
        surname, given = [part.strip() for part in text.split(",", 1)]
        return given or pd.NA, surname or pd.NA

    tokens = text.split()
    if len(tokens) == 1:
        return tokens[0], pd.NA

    return " ".join(tokens[:-1]), tokens[-1]


def split_children_names(value: Any, split_pattern: str) -> list[str]:
    if value is None or pd.isna(value):
        return []

    text = normalize_whitespace(str(value))
    if not text:
        return []

    text = re.sub(r"\([^)]*\)", " ", text)
    pieces = re.split(split_pattern, text, flags=re.IGNORECASE)
    cleaned = [normalize_whitespace(piece) for piece in pieces if normalize_whitespace(piece)]
    return cleaned


def extract_children_count_from_notes(value: Any) -> Any:
    normalized = normalize_text(value)
    if pd.isna(normalized):
        return pd.NA
    match = CHILDREN_COUNT_RE.search(normalized)
    if not match:
        return pd.NA
    return int(match.group(1))


def extract_note_flags(value: Any, config: PipelineConfig) -> dict[str, bool]:
    normalized = normalize_text(value)
    if pd.isna(normalized):
        return {
            "death_flag": False,
            "left_colony_flag": False,
            "moved_to_rio_flag": False,
        }

    return {
        "death_flag": any(keyword in normalized for keyword in config.death_keywords),
        "left_colony_flag": any(keyword in normalized for keyword in config.left_colony_keywords),
        "moved_to_rio_flag": any(keyword in normalized for keyword in config.moved_to_rio_keywords),
    }


def _build_null_tokens(config: PipelineConfig) -> set[str]:
    return {normalize_token(value) for value in config.null_like_values if normalize_token(value)}


def _standardize_base_frame(df: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
    aligned = align_to_base_schema(df)
    null_tokens = _build_null_tokens(config)

    cleaned = aligned.copy()
    for column in cleaned.columns:
        cleaned[column] = cleaned[column].map(lambda value: clean_scalar(value, null_tokens))

    return cleaned


def build_combined_records(df: pd.DataFrame, config: PipelineConfig) -> pd.DataFrame:
    combined = _standardize_base_frame(df, config)
    return reorder_columns(combined, BASE_COLUMNS + ["input_file", "input_stem", "source_row_number"])


def build_clean_records(df: pd.DataFrame, config: PipelineConfig, logger: logging.Logger | None = None) -> pd.DataFrame:
    clean = _standardize_base_frame(df, config)

    derived_components = clean["person_name_raw"].map(split_name_components)
    given_from_raw = derived_components.map(lambda item: item[0] if isinstance(item, tuple) else pd.NA)
    surname_from_raw = derived_components.map(lambda item: item[1] if isinstance(item, tuple) else pd.NA)

    effective_given = clean["given_names"].combine_first(given_from_raw)
    effective_surnames = clean["surnames"].combine_first(surname_from_raw)
    effective_person_name = clean["person_name_raw"].combine_first(clean["normalized_name"])

    clean["person_name_norm"] = effective_person_name.map(normalize_name)
    clean["given_names_norm"] = effective_given.map(normalize_name)
    clean["surnames_norm"] = effective_surnames.map(normalize_name)
    clean["normalized_name_norm"] = clean["normalized_name"].combine_first(effective_person_name).map(normalize_name)
    clean["sex_norm"] = clean["sex"].map(normalize_sex)
    clean["age_num"] = clean["age"].map(parse_age).astype("Int64")
    clean["birth_year_est_num"] = clean["birth_year_est"].map(extract_year).astype("Int64")
    clean["document_year_num"] = clean["document_year"].map(extract_year).astype("Int64")

    departure_parts = clean["departure_date"].map(normalize_date)
    clean["departure_date_norm"] = departure_parts.map(lambda item: item[0])
    clean["departure_date_precision"] = departure_parts.map(lambda item: item[1])
    clean["departure_year"] = departure_parts.map(lambda item: item[2]).astype("Int64")

    arrival_parts = clean["arrival_date"].map(normalize_date)
    clean["arrival_date_norm"] = arrival_parts.map(lambda item: item[0])
    clean["arrival_date_precision"] = arrival_parts.map(lambda item: item[1])
    clean["arrival_year"] = arrival_parts.map(lambda item: item[2]).astype("Int64")

    clean["nationality_norm"] = clean["nationality"].map(lambda value: normalize_place(value, aliases=config.country_aliases))
    clean["birthplace_norm"] = clean["birthplace"].map(normalize_place)
    clean["origin_city_norm"] = clean["origin_city"].map(lambda value: normalize_place(value, aliases=config.city_aliases))
    clean["origin_country_norm"] = clean["origin_country"].map(lambda value: normalize_place(value, aliases=config.country_aliases))
    clean["arrival_port_norm"] = clean["arrival_port"].map(normalize_place)
    clean["destination_locality_norm"] = clean["destination_locality"].map(lambda value: normalize_place(value, aliases=config.city_aliases))
    clean["father_name_norm"] = clean["father_name"].map(normalize_name)
    clean["mother_name_norm"] = clean["mother_name"].map(normalize_name)
    clean["spouse_name_norm"] = clean["spouse_name"].map(normalize_name)
    clean["ship_name_norm"] = clean["ship_name"].map(lambda value: normalize_ship_name(value, aliases=config.ship_aliases))
    clean["notes_norm"] = clean["notes"].map(normalize_text)
    clean["raw_text_norm"] = clean["raw_text"].map(normalize_text)

    parsed_children = clean["children_names"].map(lambda value: split_children_names(value, config.children_split_pattern))
    clean["children_names_list"] = parsed_children.map(lambda items: json.dumps(items, ensure_ascii=False))
    clean["children_names_norm"] = parsed_children.map(
        lambda items: "; ".join(
            normalized
            for normalized in (normalize_name(item) for item in items)
            if not pd.isna(normalized)
        )
        or pd.NA
    )

    parsed_children_count = parsed_children.map(len).astype("Int64")
    note_children_count = clean["notes"].map(extract_children_count_from_notes).astype("Int64")
    clean["children_count"] = parsed_children_count.where(parsed_children_count > 0, note_children_count)

    note_flags = clean["notes"].map(lambda value: extract_note_flags(value, config))
    clean["death_flag"] = note_flags.map(lambda item: item["death_flag"]).astype(bool)
    clean["left_colony_flag"] = note_flags.map(lambda item: item["left_colony_flag"]).astype(bool)
    clean["moved_to_rio_flag"] = note_flags.map(lambda item: item["moved_to_rio_flag"]).astype(bool)

    clean["has_spouse"] = clean["spouse_name_norm"].notna()
    clean["has_children"] = clean["children_count"].fillna(0).gt(0)

    clean["first_given_name_norm"] = clean["given_names_norm"].map(
        lambda value: value.split()[0] if isinstance(value, str) and value else pd.NA
    )
    clean["primary_surname_norm"] = clean["surnames_norm"].map(
        lambda value: value.split()[-1] if isinstance(value, str) and value else pd.NA
    )

    clean["person_name_key"] = clean["person_name_norm"]
    clean["origin_key"] = clean.apply(
        lambda row: coalesce(
            " | ".join(
                part
                for part in [row["origin_city_norm"], row["origin_country_norm"], row["birthplace_norm"]]
                if isinstance(part, str) and part
            ),
            pd.NA,
        ),
        axis=1,
    )
    clean["destination_key"] = clean.apply(
        lambda row: coalesce(
            " | ".join(
                part
                for part in [row["destination_locality_norm"], row["arrival_port_norm"]]
                if isinstance(part, str) and part
            ),
            pd.NA,
        ),
        axis=1,
    )
    clean["family_key"] = clean.apply(
        lambda row: coalesce(
            " | ".join(
                part
                for part in [row["primary_surname_norm"], row["spouse_name_norm"], row["father_name_norm"], row["mother_name_norm"]]
                if isinstance(part, str) and part
            ),
            pd.NA,
        ),
        axis=1,
    )

    clean["record_year"] = (
        clean["arrival_year"]
        .combine_first(clean["document_year_num"])
        .combine_first(clean["departure_year"])
        .combine_first(clean["birth_year_est_num"])
        .astype("Int64")
    )

    if logger:
        logger.info("Normalized %s records into the unified linkage schema.", len(clean))

    return reorder_columns(clean, FINAL_COLUMNS)


def build_quality_report(
    combined_df: pd.DataFrame,
    clean_df: pd.DataFrame,
    file_inventory: list[dict[str, Any]],
) -> dict[str, Any]:
    def safe_min(series: pd.Series) -> Any:
        valid = series.dropna()
        return int(valid.min()) if not valid.empty else None

    def safe_max(series: pd.Series) -> Any:
        valid = series.dropna()
        return int(valid.max()) if not valid.empty else None

    missing_summary = {
        column: {
            "missing": int(clean_df[column].isna().sum()),
            "filled": int(clean_df[column].notna().sum()),
        }
        for column in clean_df.columns
    }

    return {
        "records_combined": int(len(combined_df)),
        "records_clean": int(len(clean_df)),
        "files_processed": file_inventory,
        "duplicate_record_id": int(clean_df["record_id"].duplicated().sum()) if "record_id" in clean_df else 0,
        "record_year_min": safe_min(clean_df["record_year"]) if "record_year" in clean_df else None,
        "record_year_max": safe_max(clean_df["record_year"]) if "record_year" in clean_df else None,
        "has_spouse_count": int(clean_df["has_spouse"].sum()) if "has_spouse" in clean_df else 0,
        "has_children_count": int(clean_df["has_children"].sum()) if "has_children" in clean_df else 0,
        "flag_counts": {
            "death_flag": int(clean_df["death_flag"].sum()) if "death_flag" in clean_df else 0,
            "left_colony_flag": int(clean_df["left_colony_flag"].sum()) if "left_colony_flag" in clean_df else 0,
            "moved_to_rio_flag": int(clean_df["moved_to_rio_flag"].sum()) if "moved_to_rio_flag" in clean_df else 0,
        },
        "missing_summary": missing_summary,
    }
