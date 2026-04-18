from __future__ import annotations

import json
import unittest

import pandas as pd

from src.config import PipelineConfig
from src.preprocessing import (
    build_clean_records,
    extract_note_flags,
    normalize_ship_name,
    parse_age,
    split_children_names,
)
from src.schema import align_to_base_schema


class PreprocessingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = PipelineConfig()

    def test_parse_age_extracts_integer(self) -> None:
        self.assertEqual(parse_age("36 anos"), 36)
        self.assertEqual(parse_age("6 meses"), 0)

    def test_split_children_names_and_count(self) -> None:
        children = split_children_names("Anna; Karl e Maria", self.config.children_split_pattern)
        self.assertEqual(children, ["Anna", "Karl", "Maria"])

    def test_extract_note_flags(self) -> None:
        flags = extract_note_flags("Mudou-se para o Rio de Janeiro e depois faleceu.", self.config)
        self.assertTrue(flags["death_flag"])
        self.assertTrue(flags["moved_to_rio_flag"])

    def test_normalize_ship_name_removes_parentheses(self) -> None:
        self.assertEqual(normalize_ship_name("Gloriosa (Brigue dinamarquesa)"), "GLORIOSA")

    def test_align_to_base_schema_fills_missing_columns(self) -> None:
        frame = pd.DataFrame({"name": ["ANTONI, Carl"], "documentyear": ["1850"]})
        aligned = align_to_base_schema(frame)
        self.assertIn("person_name_raw", aligned.columns)
        self.assertIn("document_year", aligned.columns)
        self.assertIn("record_id", aligned.columns)

    def test_build_clean_records_generates_linkage_fields(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "record_id": "1",
                    "source": "sample.csv",
                    "document_type": "passenger_list",
                    "person_name_raw": "ANTONI, Carl",
                    "age": "36 anos",
                    "birth_year_est": "1814",
                    "origin_city": "Schleswig",
                    "ship_name": "Gloriosa (Brigue dinamarquesa)",
                    "spouse_name": "Lydie",
                    "children_names": "Anna; Karl",
                    "document_year": "1850",
                    "notes": "Mudou-se para o Rio de Janeiro.",
                    "input_file": "sample.csv",
                    "input_stem": "sample",
                    "source_row_number": "1",
                }
            ]
        )
        clean = build_clean_records(frame, self.config)
        row = clean.iloc[0]
        self.assertEqual(row["given_names_norm"], "CARL")
        self.assertEqual(row["surnames_norm"], "ANTONI")
        self.assertEqual(int(row["age_num"]), 36)
        self.assertEqual(int(row["children_count"]), 2)
        self.assertTrue(bool(row["has_spouse"]))
        self.assertTrue(bool(row["has_children"]))
        self.assertTrue(bool(row["moved_to_rio_flag"]))
        self.assertEqual(json.loads(row["children_names_list"]), ["Anna", "Karl"])
        self.assertEqual(row["ship_name_norm"], "GLORIOSA")


if __name__ == "__main__":
    unittest.main()
