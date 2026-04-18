from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


@dataclass
class PipelineConfig:
    """Central configuration for the preprocessing pipeline."""

    project_root: Path = field(default_factory=_project_root)
    input_dirs: tuple[Path, ...] | None = None
    interim_dir: Path | None = None
    processed_dir: Path | None = None
    combined_filename: str = "records_combined.csv"
    clean_filename: str = "records_clean.csv"
    quality_report_filename: str = "data_quality_report.json"
    recursive_input_search: bool = True
    min_schema_matches: int = 3
    write_parquet: bool = False
    log_level: str = "INFO"
    csv_read_encodings: tuple[str, ...] = ("utf-8-sig", "utf-8", "cp1252", "latin-1")
    csv_read_separators: tuple[str, ...] = (",", ";")
    excluded_dir_names: tuple[str, ...] = (
        "__pycache__",
        ".git",
        ".venv",
        "exploration",
        "interim",
        "processed",
        "notebooks",
    )
    excluded_file_names: tuple[str, ...] = ("records_combined.csv", "records_clean.csv")
    null_like_values: tuple[str, ...] = (
        "",
        " ",
        "-",
        "--",
        "N/A",
        "NA",
        "NAN",
        "NONE",
        "NULL",
        "SEM INFORMACAO",
        "SEM INFORMAÇÃO",
        "DESCONHECIDO",
        "DESCONHECIDA",
        "IGNORADO",
        "IGNORADA",
        "UNKNOWN",
    )
    country_aliases: dict[str, str] = field(
        default_factory=lambda: {
            "BRAZIL": "BRASIL",
            "BRASIL": "BRASIL",
            "ALEMANHA": "ALEMANHA",
            "GERMANY": "ALEMANHA",
            "DEUTSCHLAND": "ALEMANHA",
            "SUICA": "SUICA",
            "SWITZERLAND": "SUICA",
            "SUISSE": "SUICA",
            "AUSTRIA": "AUSTRIA",
            "OESTERREICH": "AUSTRIA",
            "ITALIA": "ITALIA",
            "ITALY": "ITALIA",
            "FRANCA": "FRANCA",
            "FRANCE": "FRANCA",
            "PORTUGAL": "PORTUGAL",
            "PRUSSIA": "PRUSSIA",
        }
    )
    city_aliases: dict[str, str] = field(default_factory=dict)
    ship_aliases: dict[str, str] = field(default_factory=dict)
    death_keywords: tuple[str, ...] = (
        "FALEC",
        "OBITO",
        "OBITUARIO",
        "MORTO",
        "MORTA",
        "MORREU",
        "DIED",
        "DECEASED",
    )
    left_colony_keywords: tuple[str, ...] = (
        "DEIXOU A COLONIA",
        "DEIXOU COLONIA",
        "SAIU DA COLONIA",
        "RETIROU SE DA COLONIA",
        "EVADIU SE",
        "ABANDONOU A COLONIA",
        "LEFT THE COLONY",
    )
    moved_to_rio_keywords: tuple[str, ...] = (
        "RIO DE JANEIRO",
        "MUDOU SE PARA O RIO",
        "MUDOU PARA O RIO",
        "FOI PARA O RIO",
        "SEGUIU PARA O RIO",
    )
    children_split_pattern: str = r"\s*(?:;|\||/|,|\be\b|\band\b|&)\s*"

    def __post_init__(self) -> None:
        if self.input_dirs is None:
            self.input_dirs = (
                self.project_root / "data" / "raw",
                self.project_root / "output",
            )
        else:
            self.input_dirs = tuple(Path(path) for path in self.input_dirs)

        if self.interim_dir is None:
            self.interim_dir = self.project_root / "data" / "interim"
        else:
            self.interim_dir = Path(self.interim_dir)

        if self.processed_dir is None:
            self.processed_dir = self.project_root / "data" / "processed"
        else:
            self.processed_dir = Path(self.processed_dir)

    @property
    def combined_output_path(self) -> Path:
        return self.interim_dir / self.combined_filename

    @property
    def clean_output_path(self) -> Path:
        return self.processed_dir / self.clean_filename

    @property
    def quality_report_path(self) -> Path:
        return self.processed_dir / self.quality_report_filename

    @property
    def combined_parquet_path(self) -> Path:
        return self.interim_dir / self.combined_filename.replace(".csv", ".parquet")

    @property
    def clean_parquet_path(self) -> Path:
        return self.processed_dir / self.clean_filename.replace(".csv", ".parquet")
