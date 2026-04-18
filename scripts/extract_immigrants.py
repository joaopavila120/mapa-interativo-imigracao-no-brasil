from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pdfplumber
from pypdf import PdfReader


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR = PROJECT_ROOT / "output"
CSV_PATH = OUTPUT_DIR / "immigrants_sc.csv"
REPORT_PATH = OUTPUT_DIR / "immigrants_sc_report.json"

OUTPUT_COLUMNS = [
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

SC_HEADERS = [
    "person_name_raw",
    "proced",
    "nacionalidade",
    "navio",
    "destino",
    "profissao",
    "idade",
    "estado_civil",
    "religiao",
    "ano_vol",
    "pagina",
    "sigla_fundo",
]
SC10_HEADERS = ["person_name_raw", "local", "date", "volume", "page_ref", "code_ref"]
PARANA1_HEADERS = ["person_name_raw", "year", "nationality", "book", "page_ref", "order_ref"]
PARANA2_HEADERS = ["year", "ap_number", "volume", "page_ref", "person_name_raw", "nationality"]

SC_COLLECTION = "Arquivo Publico de Santa Catarina - Indice Onomastico de Imigrantes (1859/1920)"
JOINVILLE_COLLECTION = "Arquivo Historico de Joinville - Listas de Imigrantes"
LAND_TITLES_COLLECTION = (
    "Arquivo Publico de Santa Catarina - Indice Onomastico da Serie Memoriais de Lotes, "
    "Titulos Definitivos e Provisorios de Terras (1846/1930)"
)
LAND_REQUESTS_COLLECTION = (
    "Arquivo Publico de Santa Catarina - Indice Onomastico dos Requerimentos de Concessoes de Terras "
    "da Diretoria de Terras e Colonizacao (1870/1908)"
)
COLONIAL_OFFICIOS_COLLECTION = (
    "Arquivo Publico de Santa Catarina - Indice Onomastico dos Oficios da Presidencia da Provincia "
    "para as Colonias (1875/1881)"
)
SHIP_1949_COLLECTION = (
    "Arquivo Publico de Santa Catarina - Indice Onomastico dos Imigrantes Vindos pelos Navios "
    "Hersey e McRae (1949)"
)
PASSPORT_COLLECTION = (
    "Arquivo Publico de Santa Catarina - Indice Onomastico das Series Documentais Passaportes e "
    "Carteiras de Identidades de Estrangeiros e Brasileiros (1920/1993)"
)
GOV_COLLECTION = "Arquivo Nacional - Registro de Imigrantes do Porto do Rio de Janeiro (gov.br)"
SC10_NATURALIZATION_COLLECTION = (
    "Arquivo Publico de Santa Catarina - Indice Onomastico dos Registros e Termos "
    "de Naturalizacao (1856/1963)"
)
PARANA_CATALOG_COLLECTION = (
    "Arquivo Publico do Parana - Catalogo de Documentos Referentes a Imigrantes "
    "no Estado do Parana (1861/1984)"
)
PARANA_OFICIOS_COLLECTION = (
    "Arquivo Publico do Parana - Catalogo de Documentos: Oficios e Requerimentos "
    "Referentes a Imigrantes no Estado do Parana (1854/1902)"
)

GOV_TAIL_PATTERN = re.compile(
    r"(?P<head>.+?)\s+"
    r"(?P<arrival_date>\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4})\s+"
    r"(?P<datasystem>[A-Za-z]{3}\s+\d{1,2}\s+\d{4}\s+\d{1,2}:\d{2}[AP]M)\s+"
    r"(?P<entry_number>\d+)$"
)
GOV_HEAD_PATTERN = re.compile(
    r"^(?:(?P<fund>DPMAF\s*-\s*RIO)\s+)?"
    r"(?P<notation>BRRJANRIO\.OL\.0\.RPV\.PRJ\.\d+)\s+"
    r"(?P<name_port>.+)$"
)
GOV_NULL_VALUES = {
    "",
    "-",
    "ilegivel",
    "ilegível",
    "nao consta",
    "nada consta",
    "outros",
}
GOV_STATIC_PLACE_HINTS = {
    "Antuerpia",
    "Barra do Pirai",
    "Belo Horizonte",
    "Bremen",
    "Buenos Aires",
    "Campinas",
    "Cantagalo",
    "Curitiba",
    "Fayal",
    "Genova",
    "Hamburgo",
    "Havre",
    "Ilha Terceira",
    "Itajai",
    "Juiz de Fora",
    "Leixoes",
    "Lisboa",
    "Marselha",
    "Montevideu",
    "Niteroi",
    "Paranagua",
    "Petropolis",
    "Pico",
    "Piuma",
    "Porto",
    "Porto Alegre",
    "Rio de Janeiro",
    "Rio Grande",
    "Santos",
    "Santander",
    "Sao Miguel",
    "Sao Paulo",
    "Sao Francisco",
    "Vigo",
    "Vitoria",
}
GOV_ORIGIN_COUNTRY_HINTS = {
    "antuerpia": "Belgica",
    "bergamo": "Italia",
    "bremen": "Alemanha",
    "buenos aires": "Argentina",
    "cremona": "Italia",
    "fayal": "Portugal",
    "genova": "Italia",
    "hamburgo": "Alemanha",
    "havre": "Franca",
    "ilha terceira": "Portugal",
    "leixoes": "Portugal",
    "lisboa": "Portugal",
    "mantova": "Italia",
    "marselha": "Franca",
    "montevideu": "Uruguai",
    "napoles": "Italia",
    "napoli": "Italia",
    "paraguai": "Paraguai",
    "pico": "Portugal",
    "porto": "Portugal",
    "san miguel": "Portugal",
    "santander": "Espanha",
    "sao miguel": "Portugal",
    "treviso": "Italia",
    "verona": "Italia",
    "vicenza": "Italia",
    "vigo": "Espanha",
}

COUNTRY_NAMES = {
    "apatrida",
    "alemanha",
    "arabia",
    "argentina",
    "alsacia",
    "armenia",
    "austria",
    "baviera",
    "belgica",
    "boemia",
    "brasil",
    "bulgaria",
    "checoslovaquia",
    "china",
    "dinamarca",
    "distrito federal",
    "egito",
    "escocia",
    "espanha",
    "finland",
    "franca",
    "galicia",
    "grecia",
    "hamburgo",
    "holstein",
    "hungria",
    "inglaterra",
    "irlanda",
    "iugoslavia",
    "italia",
    "japao",
    "letonia",
    "libano",
    "lombardia",
    "mecklenburg",
    "moravia",
    "noruega",
    "oldenburg",
    "polonia",
    "pomerania",
    "posen",
    "portugal",
    "prussia",
    "prussia ocidental",
    "renania",
    "reuss",
    "romenia",
    "russia",
    "saxonia",
    "silesia",
    "siria",
    "suica",
    "suecia",
    "tchecoslovaquia",
    "turingia",
    "ucrania",
    "wurttemberg",
}

DEMONYM_TO_COUNTRY = {
    "alema": "Alemanha",
    "alemao": "Alemanha",
    "apatrida": "Apatrida",
    "argentina": "Argentina",
    "argentino": "Argentina",
    "armenia": "Armenia",
    "armenia": "Armenia",
    "armenio": "Armenia",
    "austriaca": "Austria",
    "austriaco": "Austria",
    "belga": "Belgica",
    "brasileira": "Brasil",
    "brasileiro": "Brasil",
    "bulgara": "Bulgaria",
    "bulgaro": "Bulgaria",
    "checa": "Tchecoslovaquia",
    "checo": "Tchecoslovaquia",
    "chinesa": "China",
    "chines": "China",
    "dinamarques": "Dinamarca",
    "dinamarquesa": "Dinamarca",
    "egipcia": "Egito",
    "egipcio": "Egito",
    "escocesa": "Escocia",
    "escoces": "Escocia",
    "espanhola": "Espanha",
    "espanhol": "Espanha",
    "frances": "Franca",
    "francesa": "Franca",
    "grega": "Grecia",
    "grego": "Grecia",
    "hungara": "Hungria",
    "hungaro": "Hungria",
    "inglesa": "Inglaterra",
    "ingles": "Inglaterra",
    "irlandesa": "Irlanda",
    "irlandes": "Irlanda",
    "iugoslava": "Iugoslavia",
    "iugoslavo": "Iugoslavia",
    "italiana": "Italia",
    "italiano": "Italia",
    "japonesa": "Japao",
    "japones": "Japao",
    "letoniana": "Letonia",
    "letoniano": "Letonia",
    "libanesa": "Libano",
    "libanes": "Libano",
    "lombarda": "Italia",
    "lombardo": "Italia",
    "noruegues": "Noruega",
    "norueguesa": "Noruega",
    "polones": "Polonia",
    "polonesa": "Polonia",
    "portugues": "Portugal",
    "portuguesa": "Portugal",
    "prussiana": "Prussia",
    "prussiano": "Prussia",
    "romena": "Romenia",
    "romeno": "Romenia",
    "russa": "Russia",
    "russo": "Russia",
    "siria": "Siria",
    "siria": "Siria",
    "sirio": "Siria",
    "suica": "Suica",
    "suico": "Suica",
    "sueca": "Suecia",
    "sueco": "Suecia",
    "tcheca": "Tchecoslovaquia",
    "tcheco": "Tchecoslovaquia",
    "ucraniana": "Ucrania",
    "ucraniano": "Ucrania",
}

OCCUPATION_HINTS = {
    "acougueiro",
    "agricultor",
    "agrimensor",
    "agronomo",
    "açougueiro",
    "caixeiro",
    "caldeireiro",
    "capitalista",
    "candidato",
    "carpinteiro",
    "cervejeiro",
    "charuteiro",
    "chapeleiro",
    "cirurgiao",
    "comerciante",
    "construtor",
    "criada",
    "curtidor",
    "domestica",
    "economo",
    "empregado",
    "encadernador",
    "eng.",
    "engenheiro",
    "escritor",
    "estudante",
    "fabricante",
    "ferreiro",
    "funcionario",
    "fundidor",
    "inspetor",
    "jardineiro",
    "lavrador",
    "lavradora",
    "litografo",
    "maquinista",
    "marceneiro",
    "marinheiro",
    "medico",
    "mineiro",
    "militar",
    "moleiro",
    "musico",
    "oficial",
    "oleiro",
    "operaria",
    "operario",
    "padeiro",
    "pedreiro",
    "pintor",
    "sapateiro",
    "segeiro",
    "seleiro",
    "serralheiro",
    "tecelao",
    "teologo",
    "torneiro",
    "vidraceiro",
}

METADATA_PREFIXES = (
    "capitao:",
    "consignatario:",
    "saida",
    "chegada",
    "cegada",
    "destino:",
    "religiao:",
    "obs.:",
    "obs:",
    "relacao de imigrantes",
    "passageiros a bordo:",
    "nascimentos a bordo:",
    "falecimentos a bordo:",
)

NARRATIVE_PREFIXES = (
    "ass.",
    "assinado:",
    "certifico",
    "colonia",
    "declaro",
    "desterro,",
    "diretor",
    "do rio",
    "dos ",
    "hamburgo,",
    "historico",
    "joinville,",
    "manoel da costa pereira",
    "manuel da silva mafra",
    "notas bibliograficas",
    "observacao:",
    "obs.:",
    "obs:",
    "o delegado",
    "os ",
    "pelo consul",
    "recapitulacao:",
    "reparticao",
    "secretaria",
    "visto",
)


@dataclass
class SectionContext:
    ship_name: str = ""
    departure_date: str = ""
    arrival_date: str = ""
    arrival_port: str = ""
    destination_locality: str = ""
    year_hint: str = ""
    header_note: str = ""


def ascii_fold(text: str) -> str:
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", str(text))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def clean_text(text: object) -> str:
    if text is None:
        return ""
    value = str(text).replace("\u00a0", " ").replace("\x00", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def clean_cell(text: object) -> str:
    value = clean_text(text).replace("\n", "")
    return "" if value == "_" else value


def fold_letters(text: str) -> str:
    return clean_text(re.sub(r"[^a-z0-9 ]+", " ", ascii_fold(clean_text(text)).lower()))


OCCUPATION_HINTS_FOLDED = tuple(sorted(ascii_fold(keyword).lower() for keyword in OCCUPATION_HINTS))
COUNTRY_CANONICAL = {fold_letters(name): clean_text(name).title() for name in COUNTRY_NAMES}
DEMONYM_TO_COUNTRY_FOLDED = {fold_letters(key): value for key, value in DEMONYM_TO_COUNTRY.items()}


def split_name(person_name_raw: str) -> tuple[str, str]:
    raw = clean_text(person_name_raw)
    if "," in raw:
        surname, given = raw.split(",", 1)
        return clean_text(given), clean_text(surname)

    parts = raw.split()
    if len(parts) >= 2 and parts[0].isupper():
        return clean_text(" ".join(parts[1:])), clean_text(parts[0])

    return "", raw


def build_normalized_name(given_names: str, surnames: str, fallback: str) -> str:
    base = clean_text(f"{given_names} {surnames}".strip() or fallback)
    base = re.sub(r"\s*\([^)]*\)", "", base)
    base = ascii_fold(base).upper()
    base = re.sub(r"[^A-Z0-9 ]+", " ", base)
    return clean_text(base)


def make_record_id(
    source: str,
    page_reference: str,
    person_name_raw: str = "",
    ship_name: str = "",
    document_year: str = "",
    raw_text: str = "",
) -> str:
    raw_key = clean_text(raw_text)[:256]
    basis = "|".join(
        [
            clean_text(source),
            clean_text(page_reference),
            clean_text(person_name_raw),
            clean_text(ship_name),
            clean_text(document_year),
            raw_key,
        ]
    )
    digest = hashlib.sha1(basis.encode("utf-8")).hexdigest()
    return digest[:16]


def extract_years(text: str) -> list[str]:
    return re.findall(r"(18\d{2}|19\d{2}|20\d{2})", clean_text(text))


def primary_year(text: str) -> str:
    years = extract_years(text)
    if years:
        return years[0]

    short_date_match = re.search(r"\b\d{1,2}/\d{1,2}/(\d{2})\b", clean_text(text))
    if short_date_match:
        year_suffix = int(short_date_match.group(1))
        century = 18 if year_suffix >= 70 else 19
        return f"{century}{year_suffix:02d}"
    return ""


def has_single_year_reference(text: str) -> bool:
    return len(set(extract_years(text))) == 1


def estimate_birth_year(age_text: str, document_year_text: str) -> str:
    if not age_text or not has_single_year_reference(document_year_text):
        return ""

    match = re.fullmatch(r"(\d{1,3})(?:\s*anos?)?", ascii_fold(clean_text(age_text)).lower())
    if not match:
        return ""

    return str(int(primary_year(document_year_text)) - int(match.group(1)))


def country_from_nationality(nationality: str) -> str:
    value = clean_text(nationality)
    if not value:
        return ""

    folded = fold_letters(value)
    if folded in DEMONYM_TO_COUNTRY_FOLDED:
        return DEMONYM_TO_COUNTRY_FOLDED[folded]
    if folded in COUNTRY_CANONICAL:
        return COUNTRY_CANONICAL[folded]
    return ""


def looks_like_country(value: str) -> bool:
    return fold_letters(value) in COUNTRY_CANONICAL


def extract_free_text_country(value: str) -> tuple[str, str]:
    tokens = re.findall(r"[A-Za-zÀ-ÿ.\-]+", clean_text(value))
    for size in (3, 2, 1):
        for start in range(0, max(len(tokens) - size + 1, 0)):
            candidate = clean_text(" ".join(tokens[start : start + size]))
            country = country_from_nationality(candidate)
            if country:
                return candidate, country
    return "", ""


def strip_name_annotations(person_name_raw: str) -> tuple[str, list[str], str]:
    raw = clean_text(person_name_raw)
    annotations = [clean_text(match) for match in re.findall(r"\(([^)]+)\)", raw) if clean_text(match)]
    cleaned_name = clean_text(re.sub(r"\s*\([^)]*\)", "", raw))
    nationality = ""
    for annotation in annotations:
        annotation_country = country_from_nationality(annotation)
        if annotation_country and not nationality:
            nationality = annotation
            break
    return cleaned_name, annotations, nationality


def parse_parent_names_from_filiation(filiation: str) -> tuple[str, str]:
    value = clean_text(filiation)
    if not value or value == "-":
        return "", ""
    if " e " in value:
        father_name, mother_name = value.split(" e ", 1)
        return clean_text(father_name), clean_text(mother_name)
    return value, ""


def infer_sex(person_name_raw: str, detail: str = "") -> str:
    probe = ascii_fold(clean_text(f"{person_name_raw} {detail}")).lower()
    early_probe = probe.split(" c/ ", 1)[0]

    if any(marker in early_probe for marker in ("(filha)", "(mulher)", "(mae)", "solteira", "casada", "viuva")):
        return "F"
    if any(marker in early_probe for marker in ("(filho)", "(pai)", "solteiro", "casado", "viuvo")):
        return "M"
    return ""


def build_base_record(**kwargs: str) -> dict[str, str]:
    row = {column: clean_text(kwargs.get(column, "")) for column in OUTPUT_COLUMNS}
    row["record_id"] = make_record_id(
        row["source"],
        row["page_reference"],
        row["person_name_raw"],
        row["ship_name"],
        row["document_year"],
        row["raw_text"],
    )
    return row


def normalize_gov_value(value: str) -> str:
    cleaned = clean_text(value)
    if not cleaned:
        return ""
    if fold_letters(cleaned) in GOV_NULL_VALUES:
        return ""
    return cleaned


def infer_gov_origin_country(*values: str) -> str:
    for value in values:
        cleaned = normalize_gov_value(value)
        if not cleaned:
            continue

        folded = fold_letters(cleaned)
        hinted_country = GOV_ORIGIN_COUNTRY_HINTS.get(folded)
        if hinted_country:
            return hinted_country

        direct_country = country_from_nationality(cleaned)
        if direct_country:
            return direct_country

    return ""


def iter_gov_lines(reader: PdfReader, label: str) -> tuple[int, str]:
    total_pages = len(reader.pages)
    for page_number, page in enumerate(reader.pages, start=1):
        if page_number == 1 or page_number % 500 == 0 or page_number == total_pages:
            print(f"[gov] {label}: page {page_number}/{total_pages}")

        text = (page.extract_text() or "").replace("\x00", " ")
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("NomeFundo Notacao "):
                continue
            if GOV_TAIL_PATTERN.search(line):
                yield page_number, line


def build_gov_place_hint_index(reader: PdfReader) -> dict[str, list[tuple[tuple[str, ...], str]]]:
    candidates = {clean_text(value) for value in GOV_STATIC_PLACE_HINTS if clean_text(value)}

    for _, line in iter_gov_lines(reader, "cataloguing places"):
        match = GOV_TAIL_PATTERN.search(line)
        if not match:
            continue

        head = match.group("head").strip()
        segments = [clean_text(part) for part in re.split(r"\s{2,}", head) if clean_text(part)]
        if len(segments) < 4:
            continue

        for value in segments[1:4]:
            normalized = normalize_gov_value(value)
            if normalized:
                candidates.add(normalized)

    index: dict[str, list[tuple[tuple[str, ...], str]]] = {}
    for candidate in sorted(candidates):
        folded_tokens = tuple(token for token in fold_letters(candidate).split() if token)
        if not folded_tokens:
            continue
        index.setdefault(folded_tokens[-1], []).append((folded_tokens, candidate))

    for values in index.values():
        values.sort(key=lambda item: len(item[0]), reverse=True)

    return index


def split_gov_name_port(name_port: str, hint_index: dict[str, list[tuple[tuple[str, ...], str]]]) -> tuple[str, str]:
    cleaned = clean_text(name_port)
    if not cleaned:
        return "", ""

    folded = fold_letters(cleaned)
    if folded.endswith(" nada consta"):
        return clean_text(cleaned[: -len("Nada consta")]), ""

    tokens = cleaned.split()
    folded_tokens = [fold_letters(token) for token in tokens if clean_text(token)]
    if not folded_tokens:
        return "", ""

    candidates = hint_index.get(folded_tokens[-1], [])
    for hint_tokens, _ in candidates:
        hint_size = len(hint_tokens)
        if len(folded_tokens) < hint_size:
            continue
        if tuple(folded_tokens[-hint_size:]) != hint_tokens:
            continue
        person_name = clean_text(" ".join(tokens[:-hint_size]))
        departure_port = clean_text(" ".join(tokens[-hint_size:]))
        if fold_letters(person_name) in GOV_NULL_VALUES:
            person_name = ""
        return person_name, normalize_gov_value(departure_port)

    return cleaned, ""


def parse_gov_identity(person_name_raw: str) -> tuple[str, str, str]:
    cleaned_name = clean_text(person_name_raw)
    if not cleaned_name:
        return "", "", ""
    normalized_name = build_normalized_name("", cleaned_name, cleaned_name)
    return "", cleaned_name, normalized_name


def derive_gov_origin_fields(procedencia: str, departure_port: str) -> tuple[str, str, str]:
    normalized_procedencia = normalize_gov_value(procedencia)
    normalized_port = normalize_gov_value(departure_port)

    origin_country = infer_gov_origin_country(normalized_procedencia)
    procedencia_is_country = bool(normalized_procedencia and country_from_nationality(normalized_procedencia))
    port_country = infer_gov_origin_country(normalized_port)

    birthplace = normalized_procedencia or normalized_port
    origin_city = ""

    if normalized_procedencia and not procedencia_is_country:
        origin_city = normalized_procedencia
    elif normalized_port and normalized_procedencia and procedencia_is_country and (not port_country or port_country == origin_country):
        origin_city = normalized_port
    elif normalized_port and not normalized_procedencia:
        origin_city = normalized_port
        origin_country = port_country

    return birthplace, origin_city, origin_country


def split_top_level_commas(text: str) -> list[str]:
    parts: list[str] = []
    buffer: list[str] = []
    depth = 0

    for char in text:
        if char == "(":
            depth += 1
        elif char == ")" and depth > 0:
            depth -= 1

        if char == "," and depth == 0:
            part = clean_text("".join(buffer))
            if part:
                parts.append(part)
            buffer = []
            continue

        buffer.append(char)

    tail = clean_text("".join(buffer))
    if tail:
        parts.append(tail)
    return parts


def looks_like_age_token(text: str) -> bool:
    value = ascii_fold(clean_text(text)).lower()
    if not re.search(r"\d", value):
        return False

    if any(token in value for token in ("ano", "mes", "dia", " a e ")):
        return True
    return bool(re.fullmatch(r"\d+(?:\s*[1-4]/[1-4]|\s*[¼½¾])?", value))


def looks_like_occupation(text: str) -> bool:
    value = ascii_fold(clean_text(text)).lower()
    if not value:
        return False
    if "profissao nao consta" in value or "nao consta a profissao" in value:
        return True
    return any(keyword in value for keyword in OCCUPATION_HINTS_FOLDED)


def is_meta_part(text: str) -> bool:
    value = ascii_fold(clean_text(text)).lower()
    if not value:
        return False

    prefixes = (
        "†",
        "c/",
        "deixou",
        "desertou",
        "filho",
        "filha",
        "filhos",
        "filhas",
        "foram",
        "fugiu",
        "casad",
        "solteir",
        "viuv",
        "protest",
        "catol",
        "luter",
        "religiao",
        "1a classe",
        "2a classe",
        "3a classe",
        "classe",
        "recebeu",
        "receberam",
        "pagou",
        "pagaram",
        "veio",
        "vieram",
        "voltou",
        "voltaram",
        "deixou",
        "desertou",
        "fugiu",
        "ficou",
        "p/",
        "nao consta",
        "irmandade",
        "irmao",
        "enteado",
        "mae",
        "pai",
    )
    return value.startswith(prefixes)


def extract_civil_status(parts: list[str]) -> str:
    for part in parts:
        value = ascii_fold(clean_text(re.sub(r"\s*\([^)]*\)\.?\s*$", "", part))).lower()
        if value.startswith(("casad", "solteir", "viuv")):
            return clean_text(re.sub(r"\s*\([^)]*\)\.?\s*$", "", part)).rstrip(" .;")
    return ""


def extract_religion(parts: list[str]) -> str:
    for part in parts:
        value = ascii_fold(clean_text(re.sub(r"\s*\([^)]*\)\.?\s*$", "", part))).lower()
        if any(token in value for token in ("catol", "protest", "luter", "herrnhuter", "religiao")):
            return clean_text(re.sub(r"\s*\([^)]*\)\.?\s*$", "", part)).rstrip(" .;")
    return ""


def extract_travel_class(parts: list[str]) -> str:
    for part in parts:
        value = ascii_fold(clean_text(re.sub(r"\s*\([^)]*\)\.?\s*$", "", part))).lower()
        if "classe" in value or "entrecoberta" in value:
            return clean_text(re.sub(r"\s*\([^)]*\)\.?\s*$", "", part)).rstrip(" .;")
    return ""


def extract_date_like(text: str) -> str:
    value = clean_text(text)
    patterns = (
        r"\?\s*/\s*\?\s*/\s*\d{2,4}",
        r"\d{1,2}\s*ou\s*\d{1,2}/\d{1,2}/\d{2,4}",
        r"\d{1,2}\s*-\s*\d{1,2}/\d{1,2}/\d{2,4}",
        r"\d{1,2}/\d{1,2}/\d{2,4}",
    )
    for pattern in patterns:
        match = re.search(pattern, value)
        if match:
            return clean_text(match.group(0))
    return ""


def origin_from_location_parts(location_parts: list[str], nationality: str = "") -> tuple[str, str, str]:
    location_parts = [clean_text(part) for part in location_parts if clean_text(part)]
    birthplace = ", ".join(location_parts)
    origin_city = ""
    origin_country = country_from_nationality(nationality)

    if not location_parts:
        return birthplace, origin_city, origin_country

    if len(location_parts) == 1:
        if looks_like_country(location_parts[0]) and not origin_country:
            origin_country = location_parts[0]
        else:
            origin_city = location_parts[0]
        return birthplace, origin_city, origin_country

    origin_city = location_parts[0]
    if not origin_country:
        origin_country = location_parts[-1]
    return birthplace, origin_city, origin_country


def parse_sc_index(path: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []

    with pdfplumber.open(path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            if page_number < 4:
                continue

            table = page.extract_table()
            if not table:
                continue

            for row in table:
                if not row:
                    continue

                normalized_row = [clean_cell(cell) for cell in row]
                if len(normalized_row) < 12:
                    normalized_row.extend([""] * (12 - len(normalized_row)))
                normalized_row = normalized_row[:12]

                if normalized_row[0] == "SOBRENOME, NOME" or not any(normalized_row):
                    continue

                row_map = dict(zip(SC_HEADERS, normalized_row))
                person_name_raw = row_map["person_name_raw"]
                given_names, surnames = split_name(person_name_raw)
                normalized = build_normalized_name(given_names, surnames, person_name_raw)

                proced = row_map["proced"]
                nationality = row_map["nacionalidade"]
                birthplace, origin_city, origin_country = origin_from_location_parts([proced], nationality)

                notes_parts = []
                if row_map["profissao"]:
                    notes_parts.append(f"profession: {row_map['profissao']}")
                if row_map["estado_civil"]:
                    notes_parts.append(f"civil_status: {row_map['estado_civil']}")
                if row_map["religiao"]:
                    notes_parts.append(f"religion: {row_map['religiao']}")
                if row_map["ano_vol"]:
                    notes_parts.append(f"year_ref: {row_map['ano_vol']}")
                if row_map["sigla_fundo"]:
                    notes_parts.append(f"source_ref: {row_map['sigla_fundo']}")

                role_match = re.search(r"\(([^)]+)\)", person_name_raw)
                if role_match:
                    notes_parts.append(f"role_tag: {clean_text(role_match.group(1))}")

                raw_text = " | ".join(
                    f"{header}={value}"
                    for header, value in row_map.items()
                    if clean_text(value)
                )

                records.append(
                    build_base_record(
                        source=path.name,
                        source_collection=SC_COLLECTION,
                        document_type="onomastic_index_entry",
                        person_name_raw=person_name_raw,
                        given_names=given_names,
                        surnames=surnames,
                        normalized_name=normalized,
                        sex=infer_sex(person_name_raw),
                        age=row_map["idade"],
                        birth_year_est=estimate_birth_year(row_map["idade"], row_map["ano_vol"]),
                        nationality=nationality,
                        birthplace=birthplace,
                        origin_city=origin_city,
                        origin_country=origin_country,
                        ship_name=row_map["navio"],
                        departure_date="",
                        arrival_date="",
                        arrival_port="",
                        destination_locality=row_map["destino"],
                        father_name="",
                        mother_name="",
                        spouse_name="",
                        children_names="",
                        document_year=primary_year(row_map["ano_vol"]),
                        page_reference=row_map["pagina"],
                        image_reference=f"{path.name}#page={page_number}",
                        notes="; ".join(notes_parts),
                        raw_text=raw_text,
                    )
                )

    return records


def parse_sc10_naturalization_index(path: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []

    with pdfplumber.open(path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            table = page.extract_table()
            if not table or not table[0]:
                continue

            header = [clean_cell(cell) for cell in table[0]]
            header_folded = " | ".join(ascii_fold(cell).lower() for cell in header if cell)
            if "nome" not in header_folded or "vol" not in header_folded or "cod" not in header_folded:
                continue

            for row in table[1:]:
                normalized_row = [clean_cell(cell) for cell in row or []]
                if len(normalized_row) < 6:
                    normalized_row.extend([""] * (6 - len(normalized_row)))
                normalized_row = normalized_row[:6]

                if not any(normalized_row):
                    continue

                row_map = dict(zip(SC10_HEADERS, normalized_row))
                person_name_raw = row_map["person_name_raw"]
                if not person_name_raw:
                    continue

                cleaned_name, annotations, inline_nationality = strip_name_annotations(person_name_raw)
                given_names, surnames = split_name(cleaned_name)
                normalized_name = build_normalized_name(given_names, surnames, cleaned_name)

                birthplace, origin_city, origin_country, destination_locality = parse_sc10_local_field(row_map["local"])

                notes_parts = []
                if row_map["volume"]:
                    notes_parts.append(f"volume: {row_map['volume']}")
                if row_map["code_ref"]:
                    notes_parts.append(f"code_ref: {row_map['code_ref']}")
                if row_map["local"]:
                    notes_parts.append("local_field: birthplace_abroad_or_residence_in_brazil")
                for annotation in annotations:
                    notes_parts.append(f"name_tag: {annotation}")

                records.append(
                    build_base_record(
                        source=path.name,
                        source_collection=SC10_NATURALIZATION_COLLECTION,
                        document_type="naturalization_index_entry",
                        person_name_raw=cleaned_name,
                        given_names=given_names,
                        surnames=surnames,
                        normalized_name=normalized_name,
                        sex=infer_sex(cleaned_name),
                        age="",
                        birth_year_est="",
                        nationality=inline_nationality,
                        birthplace=birthplace,
                        origin_city=origin_city,
                        origin_country=origin_country or country_from_nationality(inline_nationality),
                        ship_name="",
                        departure_date="",
                        arrival_date="",
                        arrival_port="",
                        destination_locality=destination_locality,
                        father_name="",
                        mother_name="",
                        spouse_name="",
                        children_names="",
                        document_year=primary_year(row_map["date"]),
                        page_reference=row_map["page_ref"],
                        image_reference=f"{path.name}#page={page_number}",
                        notes="; ".join(notes_parts),
                        raw_text=build_table_raw_text(
                            SC10_HEADERS,
                            [
                                cleaned_name,
                                row_map["local"],
                                row_map["date"],
                                row_map["volume"],
                                row_map["page_ref"],
                                row_map["code_ref"],
                            ],
                        ),
                    )
                )

    return records


def parse_parana_catalog_index(path: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []

    with pdfplumber.open(path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            table = page.extract_table()
            if not table or not table[0]:
                continue

            header = [clean_cell(cell) for cell in table[0]]
            header_folded = " | ".join(ascii_fold(cell).lower() for cell in header if cell)
            if "nome" not in header_folded or "livro" not in header_folded or "pagina" not in header_folded:
                continue

            for row in table[1:]:
                normalized_row = [clean_cell(cell) for cell in row or []]
                if len(normalized_row) < 6:
                    normalized_row.extend([""] * (6 - len(normalized_row)))
                normalized_row = normalized_row[:6]

                row_map = dict(zip(PARANA1_HEADERS, normalized_row))
                person_name_raw = clean_text(row_map["person_name_raw"])
                if not person_name_raw:
                    continue

                cleaned_name, annotations, inline_nationality = strip_name_annotations(person_name_raw)
                given_names, surnames = split_name(cleaned_name)
                normalized_name = build_normalized_name(given_names, surnames, cleaned_name)
                nationality = inline_nationality or row_map["nationality"]

                notes_parts = []
                if row_map["book"]:
                    notes_parts.append(f"book: {row_map['book']}")
                if row_map["order_ref"]:
                    notes_parts.append(f"catalog_order: {row_map['order_ref']}")
                for annotation in annotations:
                    notes_parts.append(f"name_tag: {annotation}")

                records.append(
                    build_base_record(
                        source=path.name,
                        source_collection=PARANA_CATALOG_COLLECTION,
                        document_type="parana_immigrant_catalog_entry",
                        person_name_raw=cleaned_name,
                        given_names=given_names,
                        surnames=surnames,
                        normalized_name=normalized_name,
                        sex=infer_sex(cleaned_name),
                        age="",
                        birth_year_est="",
                        nationality=nationality,
                        birthplace="",
                        origin_city="",
                        origin_country=country_from_nationality(nationality),
                        ship_name="",
                        departure_date="",
                        arrival_date="",
                        arrival_port="",
                        destination_locality="",
                        father_name="",
                        mother_name="",
                        spouse_name="",
                        children_names="",
                        document_year=primary_year(row_map["year"]),
                        page_reference=row_map["page_ref"] or row_map["order_ref"],
                        image_reference=f"{path.name}#page={page_number}",
                        notes="; ".join(notes_parts),
                        raw_text=build_table_raw_text(
                            PARANA1_HEADERS,
                            [
                                cleaned_name,
                                row_map["year"],
                                nationality,
                                row_map["book"],
                                row_map["page_ref"],
                                row_map["order_ref"],
                            ],
                        ),
                    )
                )

    return records


def parse_parana_oficios_index(path: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []

    with pdfplumber.open(path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            table = page.extract_table()
            if not table or not table[0]:
                continue

            header = [clean_cell(cell) for cell in table[0]]
            header_folded = " | ".join(ascii_fold(cell).lower() for cell in header if cell)
            if "ano" not in header_folded or "no do ap" not in header_folded or "nacionalidade" not in header_folded:
                continue

            for row in table[1:]:
                normalized_row = [clean_cell(cell) for cell in row or []]
                if len(normalized_row) < 6:
                    normalized_row.extend([""] * (6 - len(normalized_row)))
                normalized_row = normalized_row[:6]

                row_map = dict(zip(PARANA2_HEADERS, normalized_row))
                person_name_values = split_multiline_names(row_map["person_name_raw"])
                if not person_name_values:
                    continue

                for person_name_raw in person_name_values:
                    cleaned_name, annotations, inline_nationality = strip_name_annotations(person_name_raw)
                    given_names, surnames = split_name(cleaned_name)
                    normalized_name = build_normalized_name(given_names, surnames, cleaned_name)
                    nationality = inline_nationality or row_map["nationality"]

                    notes_parts = []
                    if row_map["ap_number"]:
                        notes_parts.append(f"ap_number: {row_map['ap_number']}")
                    if row_map["volume"]:
                        notes_parts.append(f"volume: {row_map['volume']}")
                    if len(person_name_values) > 1:
                        notes_parts.append(f"group_record_size: {len(person_name_values)}")
                    for annotation in annotations:
                        notes_parts.append(f"name_tag: {annotation}")

                    records.append(
                        build_base_record(
                            source=path.name,
                            source_collection=PARANA_OFICIOS_COLLECTION,
                            document_type="parana_oficios_requerimentos_entry",
                            person_name_raw=cleaned_name,
                            given_names=given_names,
                            surnames=surnames,
                            normalized_name=normalized_name,
                            sex=infer_sex(cleaned_name),
                            age="",
                            birth_year_est="",
                            nationality=nationality,
                            birthplace="",
                            origin_city="",
                            origin_country=country_from_nationality(nationality),
                            ship_name="",
                            departure_date="",
                            arrival_date="",
                            arrival_port="",
                            destination_locality="",
                            father_name="",
                            mother_name="",
                            spouse_name="",
                            children_names="",
                            document_year=primary_year(row_map["year"]),
                            page_reference=row_map["page_ref"],
                            image_reference=f"{path.name}#page={page_number}",
                            notes="; ".join(notes_parts),
                            raw_text=build_table_raw_text(
                                PARANA2_HEADERS,
                                [
                                    row_map["year"],
                                    row_map["ap_number"],
                                    row_map["volume"],
                                    row_map["page_ref"],
                                    row_map["person_name_raw"],
                                    nationality,
                                ],
                            ),
                        )
                    )

    return records


SC7_TRAILER_RE = re.compile(
    r"(?P<year>(?:18|19|20)\d{2}|s\.\s*d\.)\s+(?P<volume>\d+[A-Za-z]*)\s+(?P<page_ref>[\dA-Za-z./-]+)$",
    re.IGNORECASE,
)
SC7_LOCALITY_STARTS = (
    "linha",
    "margem",
    "ribeirao",
    "rib.",
    "rio",
    "barra",
    "braco",
    "picada",
    "distrito",
    "urussanga",
    "azambuja",
    "guarani",
    "rodeio",
    "porto",
    "caminho",
    "salto",
    "nova",
    "bella",
    "sao",
    "theresopolis",
    "teresopolis",
    "luiz",
    "itoupava",
    "campina",
    "queimados",
    "goncalves",
    "diamantina",
    "boa",
    "fachinal",
    "aquidabam",
    "balleme",
    "palhoca",
    "blumenau",
    "brusque",
    "itajai",
    "jaragua",
    "mafra",
    "ararangua",
    "pomeranos",
)

SC9_RECORD_START_RE = re.compile(
    r"^[A-ZÀ-ÿ\[\]?'´`.,/\- ]+\s+\d{1,2}/\d{4}\s+(?:HERSEY|MCRAE|McRAE)\b",
    re.IGNORECASE,
)
SC9_RELIGION_PATTERNS = [
    ("Greco-Cat.", r"Greco-Cat\."),
    ("Armen. Greg.", r"\[?Armen\.\s*Greg\.\]?"),
    ("Ortodoxa", r"Ortodoxa"),
    ("Catolica", r"Cat[oó]lica"),
    ("Evangelica", r"Evang[eé]lica"),
    ("Protestante", r"Protestante"),
    ("Budista", r"Budista"),
]
SC9_NATIONALITY_PATTERNS = [
    (r"Po!\.\s*Ucran\.|Pol\.\s*Ucran\.", "Polonesa/Ucraniana", "Ucrania"),
    (r"Apatrida", "Apatrida", "Apatrida"),
    (r"Polones[ao]?", "Polonesa", "Polonia"),
    (r"Ucranian[ao]?|Ucran\.", "Ucraniana", "Ucrania"),
    (r"Russ[ao]?", "Russa", "Russia"),
    (r"Tchecos!?\.", "Tcheca", "Tchecoslovaquia"),
    (r"H[uú]ngar[ao]", "Hungara", "Hungria"),
    (r"Let[oô]nia|Letonian[ao]?|Lativin\.", "Letonia", "Letonia"),
    (r"Iugoslav[ao]", "Iugoslava", "Iugoslavia"),
    (r"Romen[ao]|Rumaica", "Romena", "Romenia"),
    (r"B[uú]lgar[ao]", "Bulgara", "Bulgaria"),
    (r"Greg[ao]", "Grega", "Grecia"),
]


def build_table_raw_text(headers: list[str], values: list[str]) -> str:
    return " | ".join(
        f"{header}={value}"
        for header, value in zip(headers, values)
        if clean_text(value)
    )


def split_multiline_names(value: str) -> list[str]:
    names = []
    raw_value = str(value or "").replace("\r", "\n")
    for line in raw_value.splitlines():
        cleaned_line = clean_text(line).rstrip(".; ")
        if cleaned_line:
            names.append(cleaned_line)
    return names


def parse_sc10_local_field(local_value: str) -> tuple[str, str, str, str]:
    cleaned_local = clean_text(local_value)
    if not cleaned_local:
        return "", "", "", ""

    normalized_country = country_from_nationality(cleaned_local)
    if normalized_country or looks_like_country(cleaned_local):
        return cleaned_local, "", normalized_country or cleaned_local, ""

    return "", "", "", cleaned_local


def parse_land_titles_index(path: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []

    with pdfplumber.open(path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            if page_number < 5:
                continue

            table = page.extract_table()
            if not table:
                continue

            for row in table:
                normalized_row = [clean_cell(cell) for cell in row or []]
                if len(normalized_row) < 7:
                    normalized_row.extend([""] * (7 - len(normalized_row)))
                normalized_row = normalized_row[:7]

                person_name_raw = normalized_row[0]
                if not person_name_raw or "sobrenome" in ascii_fold(person_name_raw).lower():
                    continue

                cleaned_name, annotations, inline_nationality = strip_name_annotations(person_name_raw)
                given_names, surnames = split_name(cleaned_name)
                normalized_name = build_normalized_name(given_names, surnames, cleaned_name)

                locality, year_value, book_value, page_ref, lot_value, area_value = normalized_row[1:]
                notes_parts = []
                if book_value:
                    notes_parts.append(f"book: {book_value}")
                if lot_value:
                    notes_parts.append(f"lot: {lot_value}")
                if area_value:
                    notes_parts.append(f"area: {area_value}")
                for annotation in annotations:
                    notes_parts.append(f"name_tag: {annotation}")

                records.append(
                    build_base_record(
                        source=path.name,
                        source_collection=LAND_TITLES_COLLECTION,
                        document_type="land_title_index_entry",
                        person_name_raw=cleaned_name,
                        given_names=given_names,
                        surnames=surnames,
                        normalized_name=normalized_name,
                        sex=infer_sex(cleaned_name),
                        age="",
                        birth_year_est="",
                        nationality=inline_nationality,
                        birthplace="",
                        origin_city="",
                        origin_country=country_from_nationality(inline_nationality),
                        ship_name="",
                        departure_date="",
                        arrival_date="",
                        arrival_port="",
                        destination_locality=locality,
                        father_name="",
                        mother_name="",
                        spouse_name="",
                        children_names="",
                        document_year=primary_year(year_value),
                        page_reference=page_ref,
                        image_reference=f"{path.name}#page={page_number}",
                        notes="; ".join(notes_parts),
                        raw_text=build_table_raw_text(
                            ["person_name_raw", "locality", "year", "book", "page", "lot", "area"],
                            [cleaned_name, locality, year_value, book_value, page_ref, lot_value, area_value],
                        ),
                    )
                )

    return records


def is_sc7_header_line(line: str) -> bool:
    value = clean_text(line)
    if not value:
        return True
    if re.fullmatch(r"\d{1,3}", value):
        return True
    folded = ascii_fold(value).lower()
    return folded.startswith("sobrenome, nome localidade ano vol")


def is_sc7_record_start(line: str) -> bool:
    value = clean_text(line)
    return bool(value and "," in value and not is_sc7_header_line(value))


def looks_like_sc7_locality_token(token: str) -> bool:
    value = re.sub(r"^\([^)]*\)\s*", "", clean_text(token))
    folded = ascii_fold(value).lower()
    return folded.startswith(SC7_LOCALITY_STARTS) or any(symbol in value for symbol in "/=(")


def looks_like_sc7_locality_start(text: str) -> bool:
    value = re.sub(r"^\([^)]*\)\s*", "", clean_text(text))
    folded = ascii_fold(value).lower()
    return folded.startswith(SC7_LOCALITY_STARTS) or any(symbol in value[:25] for symbol in "/=(")


def split_sc7_name_locality(prefix: str) -> tuple[str, str]:
    tokens = clean_text(prefix).split()
    comma_index = next((idx for idx, token in enumerate(tokens) if "," in token), -1)
    if comma_index < 0:
        return clean_text(prefix), ""

    min_cut = max(comma_index + 2, 2)
    max_cut = min(len(tokens) - 1, comma_index + 8)
    for cut in range(min_cut, max_cut + 1):
        name_tokens = tokens[:cut]
        locality_tokens = tokens[cut:]
        locality = clean_text(" ".join(locality_tokens))
        if not looks_like_sc7_locality_start(locality):
            continue

        if len(locality_tokens) >= 2:
            lead = locality_tokens[0]
            rest = clean_text(" ".join(locality_tokens[1:]))
            if re.fullmatch(r"\([^)]+\)", lead) and looks_like_sc7_locality_start(rest):
                return clean_text(" ".join(name_tokens + [lead])), rest
            if lead[:1].isalpha() and not looks_like_sc7_locality_token(lead) and looks_like_sc7_locality_start(rest):
                return clean_text(" ".join(name_tokens + [lead])), rest

        return clean_text(" ".join(name_tokens)), locality

    return clean_text(prefix), ""


def parse_land_request_record(path: Path, page_number: int, raw_text: str) -> dict[str, str] | None:
    match = SC7_TRAILER_RE.search(clean_text(raw_text))
    if not match:
        return None

    prefix = clean_text(raw_text[: match.start()])
    person_name_raw, locality = split_sc7_name_locality(prefix)
    leading_locality_tag = ""
    tag_match = re.match(r"^\(([^)]+)\)\s+(.*)$", locality)
    if tag_match:
        leading_locality_tag = clean_text(tag_match.group(1))
        locality = clean_text(tag_match.group(2))

    cleaned_name, annotations, inline_nationality = strip_name_annotations(person_name_raw)
    given_names, surnames = split_name(cleaned_name)
    normalized_name = build_normalized_name(given_names, surnames, cleaned_name)

    nationality = inline_nationality or leading_locality_tag
    origin_country = country_from_nationality(nationality)
    notes_parts = [f"volume: {match.group('volume')}"]
    for annotation in annotations:
        notes_parts.append(f"name_tag: {annotation}")
    if leading_locality_tag and leading_locality_tag not in annotations:
        notes_parts.append(f"name_tag: {leading_locality_tag}")

    document_year = "" if fold_letters(match.group("year")) == "s d" else primary_year(match.group("year"))

    return build_base_record(
        source=path.name,
        source_collection=LAND_REQUESTS_COLLECTION,
        document_type="land_concession_request_index_entry",
        person_name_raw=cleaned_name,
        given_names=given_names,
        surnames=surnames,
        normalized_name=normalized_name,
        sex=infer_sex(cleaned_name),
        age="",
        birth_year_est="",
        nationality=nationality,
        birthplace="",
        origin_city="",
        origin_country=origin_country,
        ship_name="",
        departure_date="",
        arrival_date="",
        arrival_port="",
        destination_locality=locality,
        father_name="",
        mother_name="",
        spouse_name="",
        children_names="",
        document_year=document_year,
        page_reference=match.group("page_ref"),
        image_reference=f"{path.name}#page={page_number}",
        notes="; ".join(notes_parts),
        raw_text=clean_text(raw_text),
    )


def parse_land_requests_index(path: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    reader = PdfReader(str(path))
    buffered_record = ""
    buffered_page = 0

    def flush_buffer() -> None:
        nonlocal buffered_record, buffered_page
        if not buffered_record:
            return
        record = parse_land_request_record(path, buffered_page, buffered_record)
        if record:
            records.append(record)
        buffered_record = ""
        buffered_page = 0

    for page_number, page in enumerate(reader.pages, start=1):
        if page_number < 5:
            continue

        text = (page.extract_text() or "").replace("\x00", " ")
        for raw_line in text.splitlines():
            line = clean_text(raw_line)
            if is_sc7_header_line(line):
                continue

            if is_sc7_record_start(line) and buffered_record and SC7_TRAILER_RE.search(buffered_record):
                flush_buffer()
                buffered_record = line
                buffered_page = page_number
                continue

            if not buffered_record:
                buffered_record = line
                buffered_page = page_number
            else:
                buffered_record = f"{buffered_record} {line}"

    flush_buffer()
    return records


def parse_colonial_officios_index(path: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []

    with pdfplumber.open(path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            if page_number < 5:
                continue

            table = page.extract_table()
            if not table:
                continue

            for row in table:
                normalized_row = [clean_cell(cell) for cell in row or []]
                if len(normalized_row) < 5:
                    normalized_row.extend([""] * (5 - len(normalized_row)))
                normalized_row = normalized_row[:5]

                person_name_raw = normalized_row[0]
                if not person_name_raw or "sobrenome" in ascii_fold(person_name_raw).lower():
                    continue

                cleaned_name, annotations, inline_nationality = strip_name_annotations(person_name_raw)
                given_names, surnames = split_name(cleaned_name)
                normalized_name = build_normalized_name(given_names, surnames, cleaned_name)

                subject_text, locality, date_value, page_ref = normalized_row[1:]
                free_marker, free_country = extract_free_text_country(subject_text)
                nationality = inline_nationality or free_marker
                origin_country = country_from_nationality(nationality) or free_country

                notes_parts = [f"subject: {subject_text}"] if subject_text else []
                for annotation in annotations:
                    notes_parts.append(f"name_tag: {annotation}")

                records.append(
                    build_base_record(
                        source=path.name,
                        source_collection=COLONIAL_OFFICIOS_COLLECTION,
                        document_type="colonial_office_letter_index_entry",
                        person_name_raw=cleaned_name,
                        given_names=given_names,
                        surnames=surnames,
                        normalized_name=normalized_name,
                        sex=infer_sex(cleaned_name, subject_text),
                        age="",
                        birth_year_est="",
                        nationality=nationality,
                        birthplace="",
                        origin_city="",
                        origin_country=origin_country,
                        ship_name="",
                        departure_date="",
                        arrival_date="",
                        arrival_port="",
                        destination_locality=locality,
                        father_name="",
                        mother_name="",
                        spouse_name="",
                        children_names="",
                        document_year=primary_year(date_value),
                        page_reference=page_ref,
                        image_reference=f"{path.name}#page={page_number}",
                        notes="; ".join(notes_parts),
                        raw_text=build_table_raw_text(
                            ["person_name_raw", "subject", "locality", "date", "page"],
                            [cleaned_name, subject_text, locality, date_value, page_ref],
                        ),
                    )
                )

    return records


def parse_passport_origin_details(raw_value: str) -> tuple[str, str, str, str, list[str]]:
    value = clean_text(raw_value).replace("–", "-").replace("—", "-")
    if not value or value == "-":
        return "", "", "", "", []

    notes: list[str] = []
    if re.search(r"naturalizad", value, re.IGNORECASE):
        notes.append("naturalized_in_brazil")

    normalized = clean_text(re.sub(r"\bNaturalizad[oa]\b", "", value, flags=re.IGNORECASE))
    parts = [clean_text(part) for part in re.split(r"\s*(?:/|-)\s*", normalized) if clean_text(part) and clean_text(part) != "-"]

    nationality = ""
    country_candidates: list[str] = []
    place_parts: list[str] = []

    for part in parts:
        folded = fold_letters(part)
        if folded in DEMONYM_TO_COUNTRY_FOLDED and not nationality:
            nationality = part
        country = country_from_nationality(part)
        if country:
            country_candidates.append(country)
        else:
            place_parts.append(part)

    non_brazil_candidates = [country for country in country_candidates if fold_letters(country) != "brasil"]
    if non_brazil_candidates:
        origin_country = non_brazil_candidates[0]
    elif country_candidates:
        origin_country = country_candidates[-1]
    else:
        origin_country = ""

    birthplace = ", ".join(place_parts)
    origin_city = place_parts[-1] if place_parts else ""
    return nationality, birthplace, origin_city, origin_country, notes


def parse_passport_index(path: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []

    with pdfplumber.open(path) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            if page_number < 5:
                continue

            table = page.extract_table()
            if not table or not table[0]:
                continue

            header = [clean_cell(cell) for cell in table[0]]
            header_folded = " | ".join(ascii_fold(cell).lower() for cell in header if cell)
            if "sobrenome, nome" not in header_folded:
                continue

            has_filiation = "filiacao" in ascii_fold(header_folded).lower()
            for row in table[1:]:
                normalized_row = [clean_cell(cell) for cell in row or []]
                if len(normalized_row) < 5:
                    normalized_row.extend([""] * (5 - len(normalized_row)))
                normalized_row = normalized_row[:5]

                person_name_raw = normalized_row[0]
                if not person_name_raw:
                    continue

                cleaned_name, annotations, inline_nationality = strip_name_annotations(person_name_raw)
                given_names, surnames = split_name(cleaned_name)
                normalized_name = build_normalized_name(given_names, surnames, cleaned_name)

                if has_filiation:
                    filiation, origin_value, birth_date, expedition_date = normalized_row[1:]
                    profession = ""
                else:
                    profession, origin_value, birth_date, expedition_date = normalized_row[1:]
                    filiation = ""

                nationality, birthplace, origin_city, origin_country, origin_notes = parse_passport_origin_details(origin_value)
                nationality = inline_nationality or nationality
                if nationality and not origin_country:
                    origin_country = country_from_nationality(nationality)

                father_name, mother_name = parse_parent_names_from_filiation(filiation)
                notes_parts = []
                if profession:
                    notes_parts.append(f"profession: {profession}")
                for note in origin_notes:
                    notes_parts.append(note)
                for annotation in annotations:
                    notes_parts.append(f"name_tag: {annotation}")

                records.append(
                    build_base_record(
                        source=path.name,
                        source_collection=PASSPORT_COLLECTION,
                        document_type="passport_or_identity_index_entry",
                        person_name_raw=cleaned_name,
                        given_names=given_names,
                        surnames=surnames,
                        normalized_name=normalized_name,
                        sex=infer_sex(cleaned_name),
                        age="",
                        birth_year_est=primary_year(birth_date),
                        nationality=nationality,
                        birthplace=birthplace,
                        origin_city=origin_city,
                        origin_country=origin_country,
                        ship_name="",
                        departure_date="",
                        arrival_date="",
                        arrival_port="",
                        destination_locality="",
                        father_name=father_name,
                        mother_name=mother_name,
                        spouse_name="",
                        children_names="",
                        document_year=primary_year(expedition_date) or primary_year(birth_date),
                        page_reference=str(page_number),
                        image_reference=f"{path.name}#page={page_number}",
                        notes="; ".join(notes_parts),
                        raw_text=build_table_raw_text(
                            ["person_name_raw", "profession_or_filiation", "origin", "birth_date", "expedition_date"],
                            [cleaned_name, profession or filiation, origin_value, birth_date, expedition_date],
                        ),
                    )
                )

    return records


def extract_sc9_religion(text: str) -> tuple[str, str]:
    for label, pattern in SC9_RELIGION_PATTERNS:
        match = re.search(rf"{pattern}\s*$", clean_text(text), re.IGNORECASE)
        if match:
            return label, clean_text(text[: match.start()])
    return "", clean_text(text)


def extract_sc9_nationality(text: str) -> tuple[str, str, str, str]:
    for pattern, label, country in SC9_NATIONALITY_PATTERNS:
        match = re.search(pattern, clean_text(text), re.IGNORECASE)
        if match:
            destination = clean_text(text[: match.start()])
            occupation = clean_text(text[match.end() :])
            return label, country, destination, occupation
    return "", "", "", clean_text(text)


def parse_sc9_record(path: Path, page_number: int, first_line: str, continuations: list[str]) -> dict[str, str] | None:
    clean_line = clean_text(first_line)
    match = re.match(
        r"^(?P<person_name_raw>.+?)\s+(?P<arrival_date>\d{1,2}/\d{4})\s+(?P<ship_name>HERSEY|MCRAE|McRAE)\s+(?P<rest>.+)$",
        clean_line,
        re.IGNORECASE,
    )
    if not match:
        return None

    rest = re.sub(r"\bI\s+(?=\d{1,2}\b)", "", clean_text(match.group("rest")))
    tail_match = re.search(r"\s+(?P<age>\d{1,2})\s+(?P<registry>[12I])\s*$", rest)
    if tail_match:
        age_value = tail_match.group("age")
        registry_code = tail_match.group("registry").replace("I", "1")
        detail_text = clean_text(rest[: tail_match.start()])
    else:
        age_value = ""
        registry_code = ""
        detail_text = rest

    religion, detail_without_religion = extract_sc9_religion(detail_text)
    nationality, origin_country, destination_locality, occupation = extract_sc9_nationality(detail_without_religion)
    continuation_text = clean_text(" ".join(continuations))
    if continuation_text:
        destination_locality = clean_text(f"{destination_locality} {continuation_text}")

    cleaned_name, annotations, inline_nationality = strip_name_annotations(match.group("person_name_raw"))
    nationality = inline_nationality or nationality
    origin_country = country_from_nationality(nationality) or origin_country
    given_names, surnames = split_name(cleaned_name)
    normalized_name = build_normalized_name(given_names, surnames, cleaned_name)

    notes_parts = []
    if occupation:
        notes_parts.append(f"profession: {occupation}")
    if religion:
        notes_parts.append(f"religion: {religion}")
    if registry_code:
        notes_parts.append(f"registry_group: {registry_code}")
    if continuation_text:
        notes_parts.append(f"continuation: {continuation_text}")
    for annotation in annotations:
        notes_parts.append(f"name_tag: {annotation}")

    return build_base_record(
        source=path.name,
        source_collection=SHIP_1949_COLLECTION,
        document_type="ship_manifest_index_entry_1949",
        person_name_raw=cleaned_name,
        given_names=given_names,
        surnames=surnames,
        normalized_name=normalized_name,
        sex=infer_sex(cleaned_name),
        age=age_value,
        birth_year_est=estimate_birth_year(age_value, match.group("arrival_date")),
        nationality=nationality,
        birthplace="",
        origin_city="",
        origin_country=origin_country,
        ship_name=match.group("ship_name").upper().replace("MCRAE", "McRAE"),
        departure_date="",
        arrival_date=match.group("arrival_date"),
        arrival_port="",
        destination_locality=destination_locality,
        father_name="",
        mother_name="",
        spouse_name="",
        children_names="",
        document_year=primary_year(match.group("arrival_date")),
        page_reference=str(page_number),
        image_reference=f"{path.name}#page={page_number}",
        notes="; ".join(notes_parts),
        raw_text=clean_text(" ".join([first_line] + continuations)),
    )


def parse_sc9_ship_index(path: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    reader = PdfReader(str(path))
    current_line = ""
    current_page = 0
    continuations: list[str] = []

    def flush_record() -> None:
        nonlocal current_line, current_page, continuations
        if not current_line:
            return
        record = parse_sc9_record(path, current_page, current_line, continuations)
        if record:
            records.append(record)
        current_line = ""
        current_page = 0
        continuations = []

    for page_number, page in enumerate(reader.pages, start=1):
        if page_number < 4:
            continue

        text = (page.extract_text() or "").replace("\x00", " ")
        for raw_line in text.splitlines():
            line = clean_text(raw_line)
            if not line:
                continue
            if SC9_RECORD_START_RE.match(line):
                flush_record()
                current_line = line
                current_page = page_number
            elif current_line:
                continuations.append(line)

        flush_record()

    return records


def extract_person_destination(detail: str) -> str:
    match = re.search(r"\bp/\s*([^;.,()]+(?:\s+[^;.,()]+){0,5})", clean_text(detail))
    return clean_text(match.group(1)) if match else ""


def extract_spouse_name(detail: str) -> str:
    patterns = (
        r"\bc/\s*(?:sua\s+)?(?:mulher|esposa|noiva)\s+([^,;]+)",
        r"\bc/\s*wife\s+([^,;]+)",
    )
    for pattern in patterns:
        match = re.search(pattern, clean_text(detail), re.IGNORECASE)
        if match:
            spouse_name = clean_text(re.sub(r"\s*\([^)]*\)", "", match.group(1)))
            spouse_name = re.sub(r"\s*†.*$", "", spouse_name)
            return clean_text(spouse_name)
    return ""


def extract_parent_names(detail: str) -> tuple[str, str]:
    clean_detail = clean_text(detail)
    father_name = ""
    mother_name = ""

    match = re.search(r"filh[ao] do\s+([^=,.;]+)\s*=\s*([^,.;]+)", clean_detail, re.IGNORECASE)
    if match:
        return clean_text(match.group(1)), clean_text(match.group(2))

    father_match = re.search(r"\bpai\s+([^,;]+)", clean_detail, re.IGNORECASE)
    mother_match = re.search(r"\bm[ãa]e\s+([^,;]+)", clean_detail, re.IGNORECASE)

    if father_match:
        father_name = clean_text(re.sub(r"\s*\([^)]*\)", "", father_match.group(1)))
    if mother_match:
        mother_name = clean_text(re.sub(r"\s*\([^)]*\)", "", mother_match.group(1)))
    return father_name, mother_name


def extract_children_names(detail: str) -> str:
    names: list[str] = []
    clean_detail = clean_text(detail)
    block_pattern = re.compile(
        r"(?:c/\s*)?(?:filho|filha|filhos|filhas)\s+(.+?)(?=(?:;\s*p/|,\s*(?:cat[oó]l|protest|solteir|casad|vi[uú]v|1ª classe|2ª classe|3ª classe|1a classe|2a classe|3a classe|recebeu|receberam|pagou|pagaram|veio|vieram|voltou|voltaram|fugiu|deixou|$))|$)",
        re.IGNORECASE,
    )

    for block_match in block_pattern.finditer(clean_detail):
        block = block_match.group(1)
        for token in split_top_level_commas(block):
            if "(" not in token:
                continue
            child_name = clean_text(token.split("(", 1)[0])
            child_name = re.sub(r"^(?:filho|filha|filhos|filhas)\s+", "", child_name, flags=re.IGNORECASE)
            child_name = clean_text(child_name)
            if child_name:
                names.append(child_name)

    deduped: list[str] = []
    seen: set[str] = set()
    for name in names:
        key = ascii_fold(name).lower()
        if key and key not in seen:
            seen.add(key)
            deduped.append(name)
    return "; ".join(deduped)


def extract_source_codes(raw_text: str) -> str:
    codes: list[str] = []
    for match in re.findall(r"\(([^()]*)\)", raw_text):
        value = clean_text(match)
        if not value or len(value) > 20:
            continue
        folded = ascii_fold(value).lower()
        if re.fullmatch(r"[a-z0-9 .&/\-]+", folded) and any(code in folded for code in ("j", "l", "d", "cf", "bg", "nt")):
            codes.append(value)
    return "; ".join(codes)


def is_year_hint(line: str) -> bool:
    return bool(re.fullmatch(r"(18|19|20)\d{2}", clean_text(line)))


def is_page_counter(line: str) -> bool:
    return bool(re.fullmatch(r"\d{1,3}", clean_text(line)))


def is_ship_header(line: str) -> bool:
    folded = ascii_fold(clean_text(line)).lower()
    return folded.startswith("navio:") or folded.startswith("vapor:")


def is_metadata_line(line: str) -> bool:
    return ascii_fold(clean_text(line)).lower().startswith(METADATA_PREFIXES)


def is_narrative_break(line: str) -> bool:
    return ascii_fold(clean_text(line)).lower().startswith(NARRATIVE_PREFIXES)


def is_index_break(line: str) -> bool:
    value = clean_text(line)
    folded = ascii_fold(value).lower()
    if "indice cronologico" in folded:
        return True
    if re.search(r"\.{10,}", value):
        return True
    return False


def looks_like_name_token(token: str) -> bool:
    token = clean_text(token)
    if not token or re.search(r"\d", token):
        return False
    stripped = token.strip("[](){}.'\"")
    return bool(stripped) and stripped[0].isalpha() and stripped[0].upper() == stripped[0]


def is_probable_person_start(line: str) -> bool:
    value = clean_text(line)
    if ":" not in value:
        return False

    prefix, _ = value.split(":", 1)
    folded = ascii_fold(prefix).lower()
    if folded.startswith(("navio", "vapor", "capitao", "consignatario", "saida", "chegada", "cegada", "destino", "religiao", "obs", "notas", "recapitulacao")):
        return False

    if "," in prefix:
        surname_part = clean_text(prefix.split(",", 1)[0])
        surname_tokens = surname_part.split()
        return 1 <= len(surname_tokens) <= 4 and all(looks_like_name_token(token) for token in surname_tokens)

    tokens = prefix.split()
    return 1 <= len(tokens) <= 4 and all(looks_like_name_token(token) for token in tokens)


def is_special_arrival_header(line: str) -> bool:
    value = clean_text(line)
    if not value.endswith(":") or is_probable_person_start(value):
        return False
    folded = ascii_fold(value).lower()
    return any(token in folded for token in ("cheg", "via ", "vieram", "atraves", "pelo vapor", "paquete"))


def extract_inline_ship_name(line: str) -> str:
    value = clean_text(line)
    patterns = (
        r"(?:vapor|paquete|navio)\s+[\"“]?([^\",:;]+)",
        r"\bc/\s*([A-Z][A-Z .\-]+?)(?=,|\s+cheg|\s+via|$)",
    )
    for pattern in patterns:
        match = re.search(pattern, value, re.IGNORECASE)
        if match:
            return clean_text(match.group(1)).strip("\"“”")
    return ""


def update_context_from_arrival_line(context: SectionContext, line: str) -> None:
    clean_line = clean_text(line)
    date_value = extract_date_like(clean_line)
    left, _, right = clean_line.partition(":")

    if date_value:
        context.arrival_date = date_value
        if not context.year_hint:
            context.year_hint = primary_year(date_value)

    location = ""
    left_folded = ascii_fold(left).lower()
    if " em " in left_folded:
        location = clean_text(left.split(" em ", 1)[1])
    elif " na " in left_folded:
        location = clean_text(left.split(" na ", 1)[1])
    elif " ao " in left_folded:
        location = clean_text(left.split(" ao ", 1)[1])

    location_folded = ascii_fold(location).lower()
    if location:
        if any(token in location_folded for token in ("porto", "francisco", "santa catarina")):
            context.arrival_port = location
        elif any(token in location_folded for token in ("colonia", "joinville", "brusque", "blumenau", "itajahy", "itajai")):
            context.destination_locality = location

    if re.search(r"na col[oô]nia(?: dona francisca)?", ascii_fold(right).lower()) and not context.destination_locality:
        context.destination_locality = "Colonia Dona Francisca"


def apply_metadata_line(context: SectionContext, line: str) -> None:
    clean_line = clean_text(line)
    folded = ascii_fold(clean_line).lower()

    if folded.startswith(("navio:", "vapor:")):
        context.ship_name = clean_text(clean_line.split(":", 1)[1])
        return
    if folded.startswith("destino:"):
        context.destination_locality = clean_text(clean_line.split(":", 1)[1])
        return
    if folded.startswith(("chegada", "cegada")):
        update_context_from_arrival_line(context, clean_line)
        return
    if folded.startswith("saida"):
        context.departure_date = extract_date_like(clean_line)
        if not context.year_hint:
            context.year_hint = primary_year(context.departure_date)
        return
    if folded.startswith(("obs", "religiao:")) and not context.header_note:
        context.header_note = clean_line


def parse_joinville_details(detail: str) -> dict[str, str]:
    clean_detail = clean_text(detail)
    parts = split_top_level_commas(clean_detail)

    age = ""
    if parts and looks_like_age_token(parts[0]):
        age = parts[0]
        parts = parts[1:]

    civil_status = extract_civil_status(parts)
    religion = extract_religion(parts)
    travel_class = extract_travel_class(parts)
    spouse_name = extract_spouse_name(clean_detail)
    father_name, mother_name = extract_parent_names(clean_detail)
    children_names = extract_children_names(clean_detail)
    destination_locality = extract_person_destination(clean_detail)

    pre_meta_parts: list[str] = []
    inline_markers = (
        " c/",
        " †",
        " filho",
        " filha",
        " filhos",
        " filhas",
        " solteir",
        " casad",
        " viuv",
        " protest",
        " catol",
        " deixou",
        " desertou",
        " fugiu",
        " foram",
        " 1a classe",
        " 2a classe",
        " 3a classe",
        " p/",
    )
    for part in parts:
        folded_part = ascii_fold(clean_text(part)).lower()
        if is_meta_part(part):
            break

        split_points = [folded_part.find(marker) for marker in inline_markers if folded_part.find(marker) > 0]
        if split_points:
            split_at = min(split_points)
            head = clean_text(part[:split_at])
            if head:
                pre_meta_parts.append(head)
            break

        pre_meta_parts.append(part)

    occupation_parts: list[str] = []
    location_parts = list(pre_meta_parts)
    while location_parts and looks_like_occupation(location_parts[0]):
        occupation_parts.append(location_parts.pop(0))

    birthplace, origin_city, origin_country = origin_from_location_parts(location_parts)
    notes_parts = []
    if occupation_parts:
        notes_parts.append(f"occupation: {', '.join(occupation_parts)}")
    if civil_status:
        notes_parts.append(f"civil_status: {civil_status}")
    if religion:
        notes_parts.append(f"religion: {religion}")
    if travel_class:
        notes_parts.append(f"travel_class: {travel_class}")

    source_codes = extract_source_codes(clean_detail)
    if source_codes:
        notes_parts.append(f"source_codes: {source_codes}")

    return {
        "age": age,
        "birthplace": birthplace,
        "origin_city": origin_city,
        "origin_country": origin_country,
        "destination_locality": destination_locality,
        "spouse_name": spouse_name,
        "father_name": father_name,
        "mother_name": mother_name,
        "children_names": children_names,
        "notes": "; ".join(notes_parts),
    }


def build_joinville_record(path: Path, page_number: int, context: SectionContext, raw_line: str) -> dict[str, str] | None:
    clean_line = clean_text(raw_line)
    if ":" not in clean_line:
        return None

    person_name_raw, detail = clean_line.split(":", 1)
    person_name_raw = clean_text(person_name_raw)
    detail = clean_text(detail)
    if not person_name_raw:
        return None

    given_names, surnames = split_name(person_name_raw)
    normalized = build_normalized_name(given_names, surnames, person_name_raw)
    sex = infer_sex(person_name_raw, detail)
    detail_map = parse_joinville_details(detail)

    document_year = primary_year(context.arrival_date) or primary_year(context.departure_date) or primary_year(context.year_hint)
    notes_parts = [detail_map["notes"]] if detail_map["notes"] else []
    if context.header_note and not context.ship_name:
        notes_parts.append(f"section_header: {context.header_note}")

    return build_base_record(
        source=path.name,
        source_collection=JOINVILLE_COLLECTION,
        document_type="translated_passenger_list_entry",
        person_name_raw=person_name_raw,
        given_names=given_names,
        surnames=surnames,
        normalized_name=normalized,
        sex=sex,
        age=detail_map["age"],
        birth_year_est=estimate_birth_year(detail_map["age"], document_year),
        nationality="",
        birthplace=detail_map["birthplace"],
        origin_city=detail_map["origin_city"],
        origin_country=detail_map["origin_country"],
        ship_name=context.ship_name,
        departure_date=context.departure_date,
        arrival_date=context.arrival_date,
        arrival_port=context.arrival_port,
        destination_locality=detail_map["destination_locality"] or context.destination_locality,
        father_name=detail_map["father_name"],
        mother_name=detail_map["mother_name"],
        spouse_name=detail_map["spouse_name"],
        children_names=detail_map["children_names"],
        document_year=document_year,
        page_reference=str(page_number),
        image_reference=f"{path.name}#page={page_number}",
        notes="; ".join(notes_parts),
        raw_text=clean_line,
    )


def parse_joinville(path: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    reader = PdfReader(str(path))
    context = SectionContext()
    buffered_record = ""
    buffered_page = 0
    seen_section = False

    def flush_buffer() -> None:
        nonlocal buffered_record, buffered_page
        if not buffered_record:
            return
        record = build_joinville_record(path, buffered_page, context, buffered_record)
        if record:
            records.append(record)
        buffered_record = ""
        buffered_page = 0

    for page_number, page in enumerate(reader.pages, start=1):
        text = (page.extract_text() or "").replace("\x00", " ")
        for raw_line in text.splitlines():
            line = clean_text(raw_line)
            if not line:
                continue

            if is_year_hint(line):
                context.year_hint = line
                continue
            if is_page_counter(line):
                continue
            if is_ship_header(line):
                flush_buffer()
                context = SectionContext(ship_name=clean_text(line.split(":", 1)[1]), year_hint=context.year_hint)
                seen_section = True
                continue
            if is_special_arrival_header(line):
                flush_buffer()
                context = SectionContext(
                    ship_name=extract_inline_ship_name(line),
                    arrival_date=extract_date_like(line),
                    year_hint=primary_year(line) or context.year_hint,
                    header_note=line,
                )
                seen_section = True
                continue
            if is_metadata_line(line):
                if not seen_section:
                    continue
                flush_buffer()
                apply_metadata_line(context, line)
                continue
            if not seen_section:
                continue
            if is_probable_person_start(line):
                flush_buffer()
                buffered_record = line
                buffered_page = page_number
                continue
            if is_narrative_break(line):
                flush_buffer()
                continue
            if is_index_break(line):
                flush_buffer()
                seen_section = False
                continue
            if buffered_record:
                buffered_record = f"{buffered_record} {line}"

    flush_buffer()
    return records


def parse_gov_pdf(path: Path) -> list[dict[str, str]]:
    reader = PdfReader(str(path))
    hint_index = build_gov_place_hint_index(reader)
    records: list[dict[str, str]] = []

    for page_number, line in iter_gov_lines(reader, "extracting rows"):
        match = GOV_TAIL_PATTERN.search(line)
        if not match:
            continue

        head = match.group("head").strip()
        arrival_date = clean_text(match.group("arrival_date"))
        datasystem = clean_text(match.group("datasystem"))
        entry_number = clean_text(match.group("entry_number"))

        segments = [clean_text(part) for part in re.split(r"\s{2,}", head) if clean_text(part)]
        if len(segments) < 4:
            continue

        identity_segment, arrival_port_raw, procedencia_raw, destination_raw = segments[:4]
        identity_match = GOV_HEAD_PATTERN.match(identity_segment)
        if not identity_match:
            continue

        fund_name = clean_text(identity_match.group("fund"))
        notation = clean_text(identity_match.group("notation"))
        name_port = clean_text(identity_match.group("name_port"))
        person_name_raw, departure_port = split_gov_name_port(name_port, hint_index)
        given_names, surnames, normalized_name = parse_gov_identity(person_name_raw)

        arrival_port = normalize_gov_value(arrival_port_raw)
        procedencia = normalize_gov_value(procedencia_raw)
        destination_locality = normalize_gov_value(destination_raw)
        birthplace, origin_city, origin_country = derive_gov_origin_fields(procedencia, departure_port)
        document_year = primary_year(arrival_date)

        note_parts = []
        if fund_name:
            note_parts.append(f"fundo: {fund_name}")
        note_parts.append(f"notacao: {notation}")
        if departure_port:
            note_parts.append(f"departure_port: {departure_port}")
        if datasystem:
            note_parts.append(f"datasistema: {datasystem}")
        if entry_number:
            note_parts.append(f"entry_number: {entry_number}")

        records.append(
            build_base_record(
                source=path.name,
                source_collection=GOV_COLLECTION,
                document_type="rio_port_arrival_registry_entry",
                person_name_raw=person_name_raw,
                given_names=given_names,
                surnames=surnames,
                normalized_name=normalized_name,
                birthplace=birthplace,
                origin_city=origin_city,
                origin_country=origin_country,
                arrival_date=arrival_date,
                arrival_port=arrival_port,
                destination_locality=destination_locality,
                document_year=document_year,
                page_reference=str(page_number),
                image_reference=f"{path.name}#page={page_number}",
                notes="; ".join(note_parts),
                raw_text=line,
            )
        )

    return records


def build_pdf_inventory() -> tuple[list[dict[str, str]], dict[str, str]]:
    inventory: list[dict[str, str]] = []
    hashes: dict[str, str] = {}
    duplicates: dict[str, str] = {}

    for pdf_path in sorted(RAW_DIR.glob("*.pdf")):
        try:
            digest = hashlib.sha256(pdf_path.read_bytes()).hexdigest()
        except OSError as exc:
            inventory.append(
                {
                    "file": pdf_path.name,
                    "sha256": "",
                    "duplicate_of": "",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            continue

        duplicate_of = hashes.get(digest, "")
        if duplicate_of:
            duplicates[pdf_path.name] = duplicate_of
        else:
            hashes[digest] = pdf_path.name

        inventory.append({"file": pdf_path.name, "sha256": digest, "duplicate_of": duplicate_of})

    return inventory, duplicates


def read_pdf_signature(path: Path) -> str:
    reader = PdfReader(str(path))
    preview = []
    for page in reader.pages[:3]:
        preview.append(clean_text(page.extract_text() or ""))
    return ascii_fold(" ".join(preview)).lower()


def detect_processor(path: Path) -> tuple[callable | None, str]:
    explicit_skips = {
        "indice.pdf": "guide/reference document without person-level records",
        "sc2.pdf": "finding aid without person-level records",
    }
    if path.name in explicit_skips:
        return None, explicit_skips[path.name]

    signature = read_pdf_signature(path)
    if path.stem.lower().startswith("joinville") or "arquivo historico de joinville" in signature:
        return parse_joinville, ""
    if path.stem.lower() == "gov" or "nomefundo notacao sobrenome porto chegada procedencia destino datachegada" in signature:
        return parse_gov_pdf, ""
    if path.stem.lower().startswith("sc10naturalizacao") or "registros e termos de naturalizacao" in signature:
        return parse_sc10_naturalization_index, ""
    if path.stem.lower().startswith("parana2") or "catalogo de documentos: oficios e requerimentos referentes a imigrantes no estado do parana" in signature:
        return parse_parana_oficios_index, ""
    if path.stem.lower().startswith("parana1") or "catalogo de documentos referentes a imigrantes no estado do parana" in signature:
        return parse_parana_catalog_index, ""
    if "passaportes e carteiras de identidades" in signature:
        return parse_passport_index, ""
    if "memoriais de lotes" in signature or "titulos definitivos e provisorios de terras" in signature:
        return parse_land_titles_index, ""
    if "requerimentos de concessoes de terras" in signature:
        return parse_land_requests_index, ""
    if "oficios da presidencia da provincia para as colonias" in signature:
        return parse_colonial_officios_index, ""
    if "navios hersey e me rae" in signature or "navios hersey e mc rae" in signature or "navios hersey e mcrae" in signature:
        return parse_sc9_ship_index, ""
    if "indice onomastico de imigrantes" in signature:
        return parse_sc_index, ""
    return None, "no compatible parser detected"


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    inventory, duplicates = build_pdf_inventory()
    processed_sources: dict[str, int] = {}
    skipped_sources: list[dict[str, str]] = []
    records: list[dict[str, str]] = []

    for pdf_path in sorted(RAW_DIR.glob("*.pdf")):
        if pdf_path.name in duplicates:
            skipped_sources.append({"file": pdf_path.name, "reason": f"duplicate of {duplicates[pdf_path.name]}"})
            continue

        try:
            parser, reason = detect_processor(pdf_path)
            if parser is None:
                skipped_sources.append({"file": pdf_path.name, "reason": reason})
                continue

            source_records = parser(pdf_path)
        except OSError as exc:
            skipped_sources.append({"file": pdf_path.name, "reason": f"{type(exc).__name__}: {exc}"})
            continue

        records.extend(source_records)
        processed_sources[pdf_path.name] = len(source_records)

    dataframe = pd.DataFrame(records, columns=OUTPUT_COLUMNS).fillna("")
    if not dataframe.empty:
        dataframe = dataframe.sort_values(
            by=["source", "document_year", "ship_name", "page_reference", "normalized_name"],
            kind="stable",
        )

    dataframe.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

    report = {
        "record_count": int(len(dataframe)),
        "processed_sources": processed_sources,
        "skipped_sources": skipped_sources,
        "pdf_inventory": inventory,
        "output_csv": str(CSV_PATH),
    }
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Wrote {len(dataframe)} records to {CSV_PATH}")
    print(f"Wrote report to {REPORT_PATH}")


if __name__ == "__main__":
    main()
