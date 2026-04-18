from __future__ import annotations

import re
from collections.abc import Iterable

import pandas as pd

BASE_COLUMNS = [
    "record_id",
    "source",
    "source_collection",
    "document_type",
    "person_name_raw",
    "given_names",
    "surnames",
    "normalized_name",
    "sex",
    "age",
    "birth_year_est",
    "nationality",
    "birthplace",
    "origin_city",
    "origin_country",
    "ship_name",
    "departure_date",
    "arrival_date",
    "arrival_port",
    "destination_locality",
    "father_name",
    "mother_name",
    "spouse_name",
    "children_names",
    "document_year",
    "page_reference",
    "image_reference",
    "notes",
    "raw_text",
]

INGESTION_COLUMNS = [
    "input_file",
    "input_stem",
    "source_row_number",
]

NORMALIZED_COLUMNS = [
    "person_name_norm",
    "given_names_norm",
    "surnames_norm",
    "normalized_name_norm",
    "sex_norm",
    "age_num",
    "birth_year_est_num",
    "document_year_num",
    "departure_date_norm",
    "departure_date_precision",
    "departure_year",
    "arrival_date_norm",
    "arrival_date_precision",
    "arrival_year",
    "nationality_norm",
    "birthplace_norm",
    "origin_city_norm",
    "origin_country_norm",
    "arrival_port_norm",
    "destination_locality_norm",
    "father_name_norm",
    "mother_name_norm",
    "spouse_name_norm",
    "children_names_list",
    "children_names_norm",
    "children_count",
    "ship_name_norm",
    "notes_norm",
    "raw_text_norm",
]

FLAG_COLUMNS = [
    "death_flag",
    "left_colony_flag",
    "moved_to_rio_flag",
    "has_spouse",
    "has_children",
]

LINKAGE_COLUMNS = [
    "first_given_name_norm",
    "primary_surname_norm",
    "person_name_key",
    "family_key",
    "origin_key",
    "destination_key",
    "record_year",
]

FINAL_COLUMNS = BASE_COLUMNS + INGESTION_COLUMNS + NORMALIZED_COLUMNS + FLAG_COLUMNS + LINKAGE_COLUMNS

_COLUMN_ALIASES = {
    "recordid": "record_id",
    "sourcefile": "source",
    "source_file": "source",
    "collection": "source_collection",
    "doctype": "document_type",
    "documenttype": "document_type",
    "name": "person_name_raw",
    "personname": "person_name_raw",
    "person_name": "person_name_raw",
    "firstname": "given_names",
    "first_name": "given_names",
    "givenname": "given_names",
    "lastname": "surnames",
    "last_name": "surnames",
    "surname": "surnames",
    "family_name": "surnames",
    "normalizedname": "normalized_name",
    "birthyear": "birth_year_est",
    "origincountry": "origin_country",
    "origincity": "origin_city",
    "destination": "destination_locality",
    "destinationlocality": "destination_locality",
    "father": "father_name",
    "mother": "mother_name",
    "spouse": "spouse_name",
    "children": "children_names",
    "documentyear": "document_year",
    "pagereference": "page_reference",
    "imagereference": "image_reference",
    "rawtext": "raw_text",
}


def normalize_column_label(column_name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", str(column_name).strip().lower())
    return cleaned.strip("_")


def column_alias_map() -> dict[str, str]:
    aliases = {normalize_column_label(column): column for column in BASE_COLUMNS}
    aliases.update(_COLUMN_ALIASES)
    return aliases


def rename_known_columns(df: pd.DataFrame) -> pd.DataFrame:
    aliases = column_alias_map()
    rename_map: dict[str, str] = {}
    for column in df.columns:
        normalized = normalize_column_label(column)
        canonical = aliases.get(normalized)
        if canonical and canonical not in rename_map.values():
            rename_map[column] = canonical
    return df.rename(columns=rename_map)


def ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    result = df.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = pd.NA
    return result


def align_to_base_schema(df: pd.DataFrame) -> pd.DataFrame:
    renamed = rename_known_columns(df)
    return ensure_columns(renamed, BASE_COLUMNS + INGESTION_COLUMNS)


def reorder_columns(df: pd.DataFrame, preferred_columns: Iterable[str]) -> pd.DataFrame:
    preferred = [column for column in preferred_columns if column in df.columns]
    extras = [column for column in df.columns if column not in preferred]
    return df.loc[:, preferred + extras]


def schema_overlap(columns: Iterable[str]) -> int:
    aliases = column_alias_map()
    return sum(1 for column in columns if normalize_column_label(str(column)) in aliases)
