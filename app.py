import hashlib
import math
import os
import pickle
import re
import unicodedata
from functools import lru_cache
from pathlib import Path

import pandas as pd
from flask import Flask, jsonify, render_template, request

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional dependency fallback
    psycopg = None
    dict_row = None

app = Flask(__name__)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 3600


@app.after_request
def apply_cache_headers(response):
    if app.debug:
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

BASE_DIR = Path(__file__).resolve().parent
PROCESSED_CSV_PATH = BASE_DIR / "data" / "processed" / "records_clean.csv"
RAW_CSV_PATH = BASE_DIR / "output" / "immigrants_sc.csv"
MAP_CACHE_DIR = BASE_DIR / "output" / "map_cache"
MAP_CACHE_VERSION = "v8"
DB_CONNECT_TIMEOUT_SECONDS = 3
DEFAULT_DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://imigracao:imigracao@localhost:5432/imigracao",
).strip()


class DatabaseNotReadyError(RuntimeError):
    pass

MAP_CONFIG = {
    "default_view": "south_brazil",
    "tile_layer": {
        "url": "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
        "attribution": (
            '&copy; <a href="https://www.openstreetmap.org/copyright">'
            "OpenStreetMap</a> contributors"
        ),
    },
    "views": {
        "south_brazil": {
            "label": "Sul do Brasil",
            "title": "História da Imigração no Sul do Brasil",
            "subtitle": (
                "Veja apenas os registros com destino ao Sul do Brasil para navegar com "
                "mais fluidez e menos sobrecarga de pontos."
            ),
            "initial_zoom": 5,
            "min_zoom": 4,
            "max_zoom": 18,
            "point_view_mode": "brazil",
            "focus_bounds": [[-33.9, -58.8], [-23.4, -47.2]],
            "max_bounds": [[-37.5, -62.8], [-21.8, -44.5]],
            "total_places_label": "Cidades mapeadas",
            "ranking_title": "Hotspots no ano atual",
            "panel_title": "Mapa de Imigracao ao Sul do Brasil",
            "panel_description": (
                "A imigracao europeia no Sul do Brasil marcou profundamente a formacao da "
                "regiao, com colonias de alemaes, italianos, poloneses e outros povos que "
                "impulsionaram o povoamento e a agricultura desde o seculo XIX."
            ),
        },
        "southeast_brazil": {
            "label": "Sudeste do Brasil",
            "title": "Historia da Imigracao no Sudeste do Brasil",
            "subtitle": (
                "Troque para o Sudeste quando quiser focar nos destinos em Rio de Janeiro, "
                "Sao Paulo, Minas Gerais e Espirito Santo."
            ),
            "point_view_mode": "brazil",
            "initial_zoom": 6,
            "min_zoom": 5,
            "max_zoom": 18,
            "focus_bounds": [[-25.6, -50.8], [-18.0, -39.2]],
            "max_bounds": [[-28.5, -54.5], [-15.5, -36.0]],
            "total_places_label": "Cidades mapeadas",
            "ranking_title": "Hotspots no ano atual",
            "panel_title": "Mapa de Imigracao ao Sudeste do Brasil",
            "panel_description": (
                "No Sudeste, a imigracao europeia esteve fortemente ligada a expansao do "
                "cafe, especialmente em Sao Paulo, atraindo milhares de trabalhadores e "
                "familias entre o fim do seculo XIX e o inicio do XX."
            ),
        },
        "portugal": {
            "label": "Portugal",
            "title": "Saidas dos emigrantes em Portugal",
            "subtitle": (
                "Carregue apenas as origens portuguesas para ver portos, ilhas e cidades "
                "de partida com destino ao Brasil."
            ),
            "point_view_mode": "europe",
            "initial_zoom": 4,
            "min_zoom": 3,
            "max_zoom": 16,
            "focus_bounds": [[30.0, -31.5], [42.5, -5.4]],
            "max_bounds": [[28.0, -33.5], [44.5, -4.0]],
            "total_places_label": "Origens mapeadas",
            "ranking_title": "Portos e origens no ano atual",
            "panel_title": "Mapa de Imigracao portuguesa para o Brasil",
            "panel_description": (
                "A imigracao portuguesa foi a mais continua da historia do Brasil, "
                "comecando na colonizacao e seguindo por seculos com forte presenca no "
                "comercio, na administracao e na vida urbana."
            ),
        },
        "italy": {
            "label": "Italia",
            "title": "Saidas dos emigrantes na Italia",
            "subtitle": (
                "Isole as origens italianas para ver melhor os pontos de partida sem "
                "sobrecarregar o restante do mapa europeu."
            ),
            "point_view_mode": "europe",
            "initial_zoom": 5,
            "min_zoom": 4,
            "max_zoom": 16,
            "focus_bounds": [[36.2, 6.0], [47.8, 18.9]],
            "max_bounds": [[35.0, 5.0], [48.8, 19.9]],
            "total_places_label": "Origens mapeadas",
            "ranking_title": "Pontos de partida na Italia",
            "panel_title": "Mapa de Imigracao italiana para o Brasil",
            "panel_description": (
                "Italianos vieram em massa para o Brasil entre o fim do seculo XIX e o "
                "inicio do XX, atuando tanto nas lavouras de cafe do Sudeste quanto nas "
                "colonias agricolas do Sul."
            ),
        },
        "germany": {
            "label": "Alemanha",
            "title": "Saidas dos emigrantes na Alemanha",
            "subtitle": (
                "Isole as origens alemas para reduzir o peso do recorte europeu e "
                "facilitar a leitura de cidades e regioes historicas."
            ),
            "point_view_mode": "europe",
            "initial_zoom": 5,
            "min_zoom": 4,
            "max_zoom": 16,
            "focus_bounds": [[46.8, 5.0], [55.8, 15.8]],
            "max_bounds": [[46.0, 4.0], [56.6, 16.6]],
            "total_places_label": "Origens mapeadas",
            "ranking_title": "Pontos de partida na Alemanha",
            "panel_title": "Mapa de Imigracao Alema para o Brasil",
            "panel_description": (
                "A imigracao alema no Brasil ganhou forca a partir de 1824, com a "
                "fundacao de Sao Leopoldo, e teve papel decisivo na ocupacao e no "
                "desenvolvimento do Sul do Brasil, especialmente em Santa Catarina."
            ),
        },
        "united_states": {
            "label": "Estados Unidos",
            "title": "Saidas dos emigrantes nos Estados Unidos",
            "subtitle": (
                "Carregue separadamente os registros ligados aos Estados Unidos para "
                "destacar rotas de partida como New York sem pesar o resto da Europa."
            ),
            "point_view_mode": "europe",
            "initial_zoom": 4,
            "min_zoom": 3,
            "max_zoom": 16,
            "focus_bounds": [[23.5, -100.5], [47.8, -65.0]],
            "max_bounds": [[18.0, -126.0], [51.5, -59.0]],
            "total_places_label": "Origens mapeadas",
            "ranking_title": "Pontos de partida nos Estados Unidos",
            "panel_title": "Mapa de Imigracao dos Estados Unidos para o Brasil",
            "panel_description": (
                "A imigracao dos Estados Unidos para o Brasil ficou historicamente marcada "
                "pela chegada de sulistas norte-americanos apos a Guerra Civil Americana, "
                "movimento incentivado pelo governo imperial brasileiro."
            ),
        },
        "europe_rest": {
            "label": "Resto da Europa",
            "title": "Saidas dos emigrantes no resto da Europa",
            "subtitle": (
                "Carregue as demais origens europeias sem misturar Portugal, Italia, "
                "Alemanha ou Estados Unidos, deixando esse recorte mais leve."
            ),
            "point_view_mode": "europe",
            "initial_zoom": 3,
            "min_zoom": 3,
            "max_zoom": 16,
            "focus_bounds": [[35.0, -10.5], [61.5, 42.5]],
            "max_bounds": [[30.0, -15.0], [67.5, 47.5]],
            "total_places_label": "Origens mapeadas",
            "ranking_title": "Pontos de partida no ano atual",
            "panel_title": "Mapa de Imigracao do resto da Europa para o Brasil",
            "panel_description": (
                "Os fluxos vindos de outras partes da Europa como Polonia e da Russia "
                "ampliaram a pluralidade da imigracao europeia no Brasil, somando novas "
                "tradicoes, religioes e formas de organizacao comunitaria."
            ),
        },
    },
}

CITY_COORDS = {
    "Angelina": {"coords": [-27.5705, -48.9848], "state": "SC"},
    "Antonina": {"coords": [-25.4280, -48.7119], "state": "PR"},
    "Ararangua": {"coords": [-28.9358, -49.4918], "state": "SC"},
    "Barra do Pirai": {"coords": [-22.4715, -43.8259], "state": "RJ"},
    "Barra Velha": {"coords": [-26.6372, -48.6842], "state": "SC"},
    "Belo Horizonte": {"coords": [-19.9245, -43.9352], "state": "MG"},
    "Blumenau": {"coords": [-26.9155, -49.0707], "state": "SC"},
    "Brusque": {"coords": [-27.0977, -48.9101], "state": "SC"},
    "Campinas": {"coords": [-22.9099, -47.0626], "state": "SP"},
    "Camboriu": {"coords": [-27.0248, -48.6543], "state": "SC"},
    "Cantagalo": {"coords": [-21.9819, -42.3692], "state": "RJ"},
    "Canoinhas": {"coords": [-26.1775, -50.3949], "state": "SC"},
    "Curitiba": {"coords": [-25.4284, -49.2733], "state": "PR"},
    "Curitibanos": {"coords": [-27.2824, -50.5816], "state": "SC"},
    "Florianopolis": {"coords": [-27.5949, -48.5482], "state": "SC"},
    "Itajai": {"coords": [-26.9101, -48.6705], "state": "SC"},
    "Itapocu": {"coords": [-26.4735, -48.7727], "state": "SC"},
    "Jaguaruna": {"coords": [-28.6144, -49.0253], "state": "SC"},
    "Joinville": {"coords": [-26.3045, -48.8487], "state": "SC"},
    "Juiz de Fora": {"coords": [-21.7642, -43.3504], "state": "MG"},
    "Lages": {"coords": [-27.8153, -50.3259], "state": "SC"},
    "Laguna": {"coords": [-28.4826, -48.7808], "state": "SC"},
    "Luiz Alves": {"coords": [-26.7234, -48.9328], "state": "SC"},
    "Mafra": {"coords": [-26.1159, -49.8086], "state": "SC"},
    "Niteroi": {"coords": [-22.8832, -43.1034], "state": "RJ"},
    "Nova Trento": {"coords": [-27.2854, -48.9294], "state": "SC"},
    "Paranagua": {"coords": [-25.5163, -48.5225], "state": "PR"},
    "Petropolis": {"coords": [-22.5050, -43.1789], "state": "RJ"},
    "Piuma": {"coords": [-20.8371, -40.7218], "state": "ES"},
    "Porto Alegre": {"coords": [-30.0346, -51.2177], "state": "RS"},
    "Porto Uniao": {"coords": [-26.2382, -51.0785], "state": "SC"},
    "Rio de Janeiro": {"coords": [-22.9068, -43.1729], "state": "RJ"},
    "Rio Grande": {"coords": [-32.0349, -52.1071], "state": "RS"},
    "Rio Negro": {"coords": [-26.1057, -49.7977], "state": "PR"},
    "Santos": {"coords": [-23.9608, -46.3336], "state": "SP"},
    "Sao Bento do Sul": {"coords": [-26.2495, -49.3786], "state": "SC"},
    "Sao Francisco do Sul": {"coords": [-26.2434, -48.6382], "state": "SC"},
    "Sao Jose": {"coords": [-27.6136, -48.6248], "state": "SC"},
    "Sao Pedro de Alcantara": {"coords": [-27.5664, -48.8051], "state": "SC"},
    "Sao Paulo": {"coords": [-23.5505, -46.6333], "state": "SP"},
    "Teresopolis": {"coords": [-27.7044, -48.8010], "state": "SC"},
    "Tijucas": {"coords": [-27.2411, -48.6346], "state": "SC"},
    "Tubarao": {"coords": [-28.4717, -49.0144], "state": "SC"},
    "Vitoria": {"coords": [-20.3155, -40.3128], "state": "ES"},
}

EXACT_LOCALITY_ALIASES = {
    "barra do pirai": "Barra do Pirai",
    "belo horizonte": "Belo Horizonte",
    "campinas": "Campinas",
    "cantagalo": "Cantagalo",
    "colonia": "Joinville",
    "colonia dona francisca": "Joinville",
    "colonia brusque": "Brusque",
    "corte": "Rio de Janeiro",
    "desterro": "Florianopolis",
    "destrrro": "Florianopolis",
    "distrito federal": "Rio de Janeiro",
    "dona francisca": "Joinville",
    "espirito santo": "Vitoria",
    "itajahy": "Itajai",
    "itajai": "Itajai",
    "itapocu": "Itapocu",
    "itapocu ou curitiba": "Itapocu",
    "itapocu ou curitiba em": "Itapocu",
    "juiz de fora": "Juiz de Fora",
    "niteroi": "Niteroi",
    "parana": "Curitiba",
    "paranagua": "Paranagua",
    "petropolis": "Petropolis",
    "piuma": "Piuma",
    "rio de janeiro": "Rio de Janeiro",
    "santos": "Santos",
    "sao paulo": "Sao Paulo",
    "vitoria": "Vitoria",
}

PARTIAL_LOCALITY_ALIASES = [
    ("colonia dona francisca", "Joinville"),
    ("colonia d francisca", "Joinville"),
    ("col d francisca", "Joinville"),
    ("c d francisca", "Joinville"),
    ("c d frrancisca", "Joinville"),
    ("dona francisca", "Joinville"),
    ("joinville", "Joinville"),
    ("col p dom pedro", "Brusque"),
    ("nova trento", "Nova Trento"),
    ("n trento", "Nova Trento"),
    ("brusque", "Brusque"),
    ("itajahy", "Itajai"),
    ("itajai", "Itajai"),
    ("blumenau", "Blumenau"),
    ("sao francisco", "Sao Francisco do Sul"),
    ("s francisco", "Sao Francisco do Sul"),
    ("sao bento", "Sao Bento do Sul"),
    ("mafra", "Mafra"),
    ("florianopolis", "Florianopolis"),
    ("desterro", "Florianopolis"),
    ("laguna", "Laguna"),
    ("lages", "Lages"),
    ("angelina", "Angelina"),
    ("curitibanos", "Curitibanos"),
    ("canoinhas", "Canoinhas"),
    ("curitiba", "Curitiba"),
    ("porto alegre", "Porto Alegre"),
    ("rio negro", "Rio Negro"),
    ("antonina", "Antonina"),
    ("paranagua", "Paranagua"),
    ("tijucas", "Tijucas"),
    ("sao jose", "Sao Jose"),
    ("praia de fora", "Sao Jose"),
    ("santa isabel", "Sao Pedro de Alcantara"),
    ("col s isabel", "Sao Pedro de Alcantara"),
    ("teresopolis", "Teresopolis"),
    ("tubarao", "Tubarao"),
    ("ararangua", "Ararangua"),
    ("porto uniao", "Porto Uniao"),
    ("barra do pirai", "Barra do Pirai"),
    ("barra velha", "Barra Velha"),
    ("belo horizonte", "Belo Horizonte"),
    ("camboriu", "Camboriu"),
    ("campinas", "Campinas"),
    ("cantagalo", "Cantagalo"),
    ("corte", "Rio de Janeiro"),
    ("distrito federal", "Rio de Janeiro"),
    ("espirito santo", "Vitoria"),
    ("luiz alves", "Luiz Alves"),
    ("jaguaruna", "Jaguaruna"),
    ("juiz de fora", "Juiz de Fora"),
    ("niteroi", "Niteroi"),
    ("parana", "Curitiba"),
    ("paranagua", "Paranagua"),
    ("petropolis", "Petropolis"),
    ("piuma", "Piuma"),
    ("rio de janeiro", "Rio de Janeiro"),
    ("rio grande do sul", "Rio Grande"),
    ("rio grande do s", "Rio Grande"),
    ("rio grande", "Rio Grande"),
    ("santos", "Santos"),
    ("sao paulo", "Sao Paulo"),
    ("itapocu", "Itapocu"),
    ("vitoria", "Vitoria"),
]

IGNORED_SURNAME_KEYS = {"ass", "obs", "indice", "relacao"}
BRAZIL_PATTERN = re.compile(r"\b(brasil|brazil|brasileir[oa]?)\b", re.IGNORECASE)
COUNTRY_FILTER_RULES = [
    (("ITALIA", "ITALIANO", "ITALIANA"), "italia", "Italia"),
    (("AUSTRIA", "AUSTRIAC", "TIROL", "BOEMIA", "BOHEM", "MORAV"), "austria", "Austria"),
    (("PORTUGAL", "PORTUGUES"), "portugal", "Portugal"),
    (
        (
            "ALEMANHA",
            "GERMANY",
            "DEUTSCH",
            "PRUSS",
            "POMERAN",
            "SAXON",
            "BAVIER",
            "HAMBURG",
            "HAMBURGO",
            "HOLSTEIN",
            "HANNOVER",
            "HESSEN",
            "HESSE",
            "MECKLENB",
            "BRAUNSCHWEIG",
            "WURTTEMBERG",
            "BADEN",
            "WESTFAL",
            "SCHLESWIG",
            "THURING",
            "WEIMAR",
            "KONIGSBERG",
            "KOENIGSBERG",
            "LUBECK",
            "BREMEN",
        ),
        "alemanha",
        "Alemanha",
    ),
    (("SUICA", "SWITZERLAND", "SUISSE", "SUICO", "SCHWEIZ"), "suica", "Suica"),
    (("FRANCA", "FRANCE", "FRANCES"), "franca", "Franca"),
    (("ESPANHA", "ESPANHOL", "SPAIN"), "espanha", "Espanha"),
    (("RUSSIA", "RUSSO", "RUSSLAND"), "russia", "Russia"),
    (("POLONIA", "POLONES", "POLAND"), "polonia", "Polonia"),
    (("DINAMARCA", "DENMARK", "DINAMARQUES"), "dinamarca", "Dinamarca"),
    (("BELGICA", "BELGIUM", "BELGA"), "belgica", "Belgica"),
    (("HOLANDA", "HOLLAND", "NETHERLAND", "PAISES BAIXOS"), "holanda", "Holanda"),
    (("LUXEMBURGO", "LUXEMBOURG"), "luxemburgo", "Luxemburgo"),
    (("SUECIA", "SWEDEN", "SUECO"), "suecia", "Suecia"),
    (("NORUEGA", "NORWAY", "NORUEGUES"), "noruega", "Noruega"),
    (("INGLATERRA", "ENGLAND", "REINO UNIDO", "UNITED KINGDOM", "BRIT"), "reino-unido", "Reino Unido"),
    (("ESTADOS UNIDOS", "UNITED STATES", "AMERICA", "EUA", "USA"), "estados-unidos", "Estados Unidos"),
]

MAP_CSV_COLUMNS = {
    "record_id",
    "source",
    "source_collection",
    "document_type",
    "person_name_raw",
    "given_names",
    "given_names_norm",
    "surnames",
    "surnames_norm",
    "sex",
    "sex_norm",
    "age",
    "age_num",
    "nationality",
    "nationality_norm",
    "birthplace",
    "birthplace_norm",
    "origin_city",
    "origin_city_norm",
    "origin_country",
    "origin_country_norm",
    "ship_name",
    "ship_name_norm",
    "departure_date",
    "departure_date_norm",
    "arrival_date",
    "arrival_date_norm",
    "arrival_port",
    "arrival_port_norm",
    "destination_locality",
    "document_year",
    "page_reference",
    "notes",
}

EUROPE_LOCATION_COORDS = {
    "Alemanha": {"coords": [51.1657, 10.4515], "state": "Alemanha"},
    "Altona": {"coords": [53.5511, 9.9350], "state": "Alemanha"},
    "Amsterdam": {"coords": [52.3676, 4.9041], "state": "Holanda"},
    "Antuerpia": {"coords": [51.2194, 4.4025], "state": "Belgica"},
    "Austria": {"coords": [48.2082, 16.3738], "state": "Austria"},
    "Baden": {"coords": [48.5000, 8.5000], "state": "Alemanha"},
    "Baviera": {"coords": [48.7904, 11.4979], "state": "Alemanha"},
    "Belgica": {"coords": [50.8503, 4.3517], "state": "Belgica"},
    "Berlim": {"coords": [52.5200, 13.4050], "state": "Alemanha"},
    "Boemia": {"coords": [50.0755, 14.4378], "state": "Boemia"},
    "Braunschweig": {"coords": [52.2689, 10.5268], "state": "Alemanha"},
    "Breslau": {"coords": [51.1079, 17.0385], "state": "Silesia"},
    "Chemnitz": {"coords": [50.8278, 12.9214], "state": "Saxonia"},
    "Copenhagen": {"coords": [55.6761, 12.5683], "state": "Dinamarca"},
    "Dassel": {"coords": [51.8014, 9.6906], "state": "Alemanha"},
    "Dinamarca": {"coords": [55.6761, 12.5683], "state": "Dinamarca"},
    "Dresden": {"coords": [51.0504, 13.7373], "state": "Saxonia"},
    "Espanha": {"coords": [40.4168, -3.7038], "state": "Espanha"},
    "Estocolmo": {"coords": [59.3293, 18.0686], "state": "Suecia"},
    "Estados Unidos": {"coords": [39.8283, -98.5795], "state": "Estados Unidos"},
    "Franca": {"coords": [48.8566, 2.3522], "state": "Franca"},
    "Galicia": {"coords": [49.8397, 24.0297], "state": "Galicia"},
    "Genova": {"coords": [44.4056, 8.9463], "state": "Italia"},
    "Glauchau": {"coords": [50.8195, 12.5442], "state": "Saxonia"},
    "Gorlitz": {"coords": [51.1526, 14.9872], "state": "Alemanha"},
    "Hamburgo": {"coords": [53.5511, 9.9937], "state": "Alemanha"},
    "Hannover": {"coords": [52.3759, 9.7320], "state": "Alemanha"},
    "Helmstedt": {"coords": [52.2276, 11.0098], "state": "Alemanha"},
    "Holanda": {"coords": [52.3676, 4.9041], "state": "Holanda"},
    "Holstein": {"coords": [54.3233, 10.1228], "state": "Holstein"},
    "Ilha da Madeira": {"coords": [32.7607, -16.9595], "state": "Portugal"},
    "Ilha Terceira": {"coords": [38.7223, -27.2206], "state": "Portugal"},
    "Italia": {"coords": [41.9028, 12.4964], "state": "Italia"},
    "Kiel": {"coords": [54.3233, 10.1228], "state": "Alemanha"},
    "Leipzig": {"coords": [51.3397, 12.3731], "state": "Saxonia"},
    "Leixoes": {"coords": [41.1854, -8.7000], "state": "Portugal"},
    "Lisboa": {"coords": [38.7223, -9.1393], "state": "Portugal"},
    "Luxemburgo": {"coords": [49.6116, 6.1319], "state": "Luxemburgo"},
    "Liverpool": {"coords": [53.4084, -2.9916], "state": "Reino Unido"},
    "Londres": {"coords": [51.5074, -0.1278], "state": "Reino Unido"},
    "Magdeburg": {"coords": [52.1205, 11.6276], "state": "Alemanha"},
    "Mantova": {"coords": [45.1564, 10.7914], "state": "Italia"},
    "Marseille": {"coords": [43.2965, 5.3698], "state": "Franca"},
    "Mecklenburg": {"coords": [53.6127, 11.4294], "state": "Alemanha"},
    "Moravia": {"coords": [49.1951, 16.6068], "state": "Moravia"},
    "Munique": {"coords": [48.1374, 11.5755], "state": "Alemanha"},
    "Napoles": {"coords": [40.8518, 14.2681], "state": "Italia"},
    "New York": {"coords": [40.7128, -74.0060], "state": "Estados Unidos"},
    "Noruega": {"coords": [59.9139, 10.7522], "state": "Noruega"},
    "Oldenburg": {"coords": [53.1435, 8.2146], "state": "Alemanha"},
    "Polonia": {"coords": [52.2297, 21.0122], "state": "Polonia"},
    "Pomerania": {"coords": [53.4285, 14.5528], "state": "Pomerania"},
    "Porto": {"coords": [41.1579, -8.6291], "state": "Portugal"},
    "Portugal": {"coords": [38.7223, -9.1393], "state": "Portugal"},
    "Prussia": {"coords": [52.5200, 13.4050], "state": "Prussia"},
    "Prussia Ocidental": {"coords": [53.0138, 18.5984], "state": "Prussia Ocidental"},
    "Russia": {"coords": [55.7558, 37.6173], "state": "Russia"},
    "Reino Unido": {"coords": [51.5074, -0.1278], "state": "Reino Unido"},
    "Saxonia": {"coords": [51.0504, 13.7373], "state": "Saxonia"},
    "Sao Miguel": {"coords": [37.7412, -25.6756], "state": "Portugal"},
    "Schleitz": {"coords": [50.5782, 11.8124], "state": "Alemanha"},
    "Silesia": {"coords": [51.1079, 17.0385], "state": "Silesia"},
    "Suica": {"coords": [46.9480, 7.4474], "state": "Suica"},
    "Suecia": {"coords": [59.3293, 18.0686], "state": "Suecia"},
    "Stuttgart": {"coords": [48.7758, 9.1829], "state": "Alemanha"},
    "Southampton": {"coords": [50.9097, -1.4044], "state": "Reino Unido"},
    "Trieste": {"coords": [45.6495, 13.7768], "state": "Italia"},
    "Treviso": {"coords": [45.6669, 12.2430], "state": "Italia"},
    "Veneza": {"coords": [45.4408, 12.3155], "state": "Italia"},
    "Verona": {"coords": [45.4384, 10.9916], "state": "Italia"},
    "Vicenza": {"coords": [45.5455, 11.5354], "state": "Italia"},
    "Weimar": {"coords": [50.9795, 11.3235], "state": "Alemanha"},
    "Westfalia": {"coords": [51.4818, 7.2162], "state": "Westfalia"},
}

EUROPE_RAW_EXACT_ALIASES = {
    "alemanha": "Alemanha",
    "altona": "Altona",
    "amsterdam": "Amsterdam",
    "antuerpia": "Antuerpia",
    "antuerpia belgica": "Antuerpia",
    "austria": "Austria",
    "baden": "Baden",
    "baviera": "Baviera",
    "belgica": "Belgica",
    "berlim": "Berlim",
    "boemia": "Boemia",
    "braunschweig": "Braunschweig",
    "breslau": "Breslau",
    "chemnitz": "Chemnitz",
    "copenhagen": "Copenhagen",
    "dassel": "Dassel",
    "dinamarca": "Dinamarca",
    "dresden": "Dresden",
    "espanha": "Espanha",
    "estocolmo": "Estocolmo",
    "estados unidos": "Estados Unidos",
    "franca": "Franca",
    "galicia": "Galicia",
    "genoa": "Genova",
    "genua": "Genova",
    "genova": "Genova",
    "glauchau": "Glauchau",
    "gorlitz": "Gorlitz",
    "görlitz": "Gorlitz",
    "hamburgo": "Hamburgo",
    "hannover": "Hannover",
    "hanover": "Hannover",
    "helmstedt": "Helmstedt",
    "holanda": "Holanda",
    "holstein": "Holstein",
    "ilha da madeira": "Ilha da Madeira",
    "ilha terceira": "Ilha Terceira",
    "italia": "Italia",
    "kiel": "Kiel",
    "leixoes": "Leixoes",
    "leipzig": "Leipzig",
    "lisboa": "Lisboa",
    "luxemburg": "Luxemburgo",
    "luxemburgo": "Luxemburgo",
    "liverpool": "Liverpool",
    "london": "Londres",
    "londres": "Londres",
    "magdeburg": "Magdeburg",
    "mantova": "Mantova",
    "marseille": "Marseille",
    "marselha": "Marseille",
    "mecklenburg": "Mecklenburg",
    "meklemburgo": "Mecklenburg",
    "moravia": "Moravia",
    "moravia silesia": "Moravia",
    "munique": "Munique",
    "napoles": "Napoles",
    "napoli": "Napoles",
    "new york": "New York",
    "nova york": "New York",
    "noruega": "Noruega",
    "oldenburg": "Oldenburg",
    "polonia": "Polonia",
    "pomerania": "Pomerania",
    "porto": "Porto",
    "portugal": "Portugal",
    "prussia": "Prussia",
    "prussia ocidental": "Prussia Ocidental",
    "r saxonia": "Saxonia",
    "reino unido": "Reino Unido",
    "russia": "Russia",
    "s miguel": "Sao Miguel",
    "sao miguel": "Sao Miguel",
    "san miguel": "Sao Miguel",
    "saxonia": "Saxonia",
    "schleitz": "Schleitz",
    "silesia": "Silesia",
    "suica": "Suica",
    "suecia": "Suecia",
    "stuttgart": "Stuttgart",
    "southampton": "Southampton",
    "trieste": "Trieste",
    "treviso": "Treviso",
    "venezia": "Veneza",
    "veneza": "Veneza",
    "venice": "Veneza",
    "verona": "Verona",
    "vicenza": "Vicenza",
    "england": "Reino Unido",
    "eua": "Estados Unidos",
    "america": "Estados Unidos",
    "america do norte": "Estados Unidos",
    "inglaterra": "Reino Unido",
    "netherlands": "Holanda",
    "paises baixos": "Holanda",
    "holland": "Holanda",
    "usa": "Estados Unidos",
    "weimar": "Weimar",
    "westfalia": "Westfalia",
    "westphalia": "Westfalia",
}

EUROPE_RAW_PARTIAL_ALIASES = [
    ("amster", "Amsterdam"),
    ("antuerpia", "Antuerpia"),
    ("austr", "Austria"),
    ("baden", "Baden"),
    ("bavier", "Baviera"),
    ("belg", "Belgica"),
    ("berlim", "Berlim"),
    ("boem", "Boemia"),
    ("braunschweig", "Braunschweig"),
    ("breslau", "Breslau"),
    ("chemnitz", "Chemnitz"),
    ("copenhagen", "Copenhagen"),
    ("dassel", "Dassel"),
    ("dinamar", "Dinamarca"),
    ("dresden", "Dresden"),
    ("espanh", "Espanha"),
    ("estocolmo", "Estocolmo"),
    ("estados unidos", "Estados Unidos"),
    ("franc", "Franca"),
    ("galicia", "Galicia"),
    ("genoa", "Genova"),
    ("genua", "Genova"),
    ("genov", "Genova"),
    ("glauchau", "Glauchau"),
    ("gorlitz", "Gorlitz"),
    ("hamburg", "Hamburgo"),
    ("hamburgo", "Hamburgo"),
    ("hanover", "Hannover"),
    ("hannover", "Hannover"),
    ("holland", "Holanda"),
    ("holstein", "Holstein"),
    ("ilha terceira", "Ilha Terceira"),
    ("ital", "Italia"),
    ("kiel", "Kiel"),
    ("leix", "Leixoes"),
    ("leipzig", "Leipzig"),
    ("lisboa", "Lisboa"),
    ("luxemb", "Luxemburgo"),
    ("liverp", "Liverpool"),
    ("london", "Londres"),
    ("londres", "Londres"),
    ("magdeburg", "Magdeburg"),
    ("madeira", "Ilha da Madeira"),
    ("mantov", "Mantova"),
    ("marselh", "Marseille"),
    ("marseill", "Marseille"),
    ("mecklenburg", "Mecklenburg"),
    ("morav", "Moravia"),
    ("muniqu", "Munique"),
    ("napol", "Napoles"),
    ("new york", "New York"),
    ("nova york", "New York"),
    ("norueg", "Noruega"),
    ("oldenburg", "Oldenburg"),
    ("polon", "Polonia"),
    ("pomeran", "Pomerania"),
    ("porto", "Porto"),
    ("portug", "Portugal"),
    ("prussia ocidental", "Prussia Ocidental"),
    ("prussia oriental", "Prussia"),
    ("prussia", "Prussia"),
    ("preussen", "Prussia"),
    ("r saxonia", "Saxonia"),
    ("reino unido", "Reino Unido"),
    ("russia", "Russia"),
    ("russ", "Russia"),
    ("eua", "Estados Unidos"),
    ("s miguel", "Sao Miguel"),
    ("sao miguel", "Sao Miguel"),
    ("san miguel", "Sao Miguel"),
    ("saxon", "Saxonia"),
    ("saxonia", "Saxonia"),
    ("schleitz", "Schleitz"),
    ("siles", "Silesia"),
    ("suic", "Suica"),
    ("suec", "Suecia"),
    ("stuttgart", "Stuttgart"),
    ("southampt", "Southampton"),
    ("terceira", "Ilha Terceira"),
    ("triest", "Trieste"),
    ("trevis", "Treviso"),
    ("usa", "Estados Unidos"),
    ("america do norte", "Estados Unidos"),
    ("america", "Estados Unidos"),
    ("venez", "Veneza"),
    ("verona", "Verona"),
    ("vicenza", "Vicenza"),
    ("weimar", "Weimar"),
    ("westf", "Westfalia"),
    ("westph", "Westfalia"),
]

EUROPE_NOISE_FRAGMENTS = (
    "ao todo",
    "capitalista",
    "agricultor",
    "agricultora",
    "alfaiate",
    "colono",
    "criada",
    "criado",
    "cordoeiro",
    "fazenda",
    "ilegivel",
    "latoeiro",
    "lavrador",
    "lavradora",
    "naturalista",
    "pessoas",
    "professor",
    "religiao nao consta",
    "tanoeiro",
)

OCCUPATION_NOISE_KEYS = {
    "agricultor",
    "agricultora",
    "alfaiate",
    "capitalista",
    "colono",
    "cordoeiro",
    "criada",
    "criado",
    "lavrador",
    "lavradora",
    "latoeiro",
    "naturalista",
    "professor",
    "tanoeiro",
}

BRAZIL_LOCATION_HINTS = (
    "azambuja",
    "brasil",
    "curitiba",
    "desterro",
    "florianopolis",
    "iguape",
    "joinville",
    "niteroi",
    "parana",
    "porto alegre",
    "rio de janeiro",
    "rio grande do sul",
    "santa catarina",
    "santos",
    "sao francisco",
    "sao paulo",
)

VALID_YEAR_MIN = 1750
VALID_YEAR_MAX = 1995
SOUTH_BRAZIL_STATES = {"SC", "PR", "RS"}
SOUTHEAST_BRAZIL_STATES = {"ES", "MG", "RJ", "SP"}
EUROPE_COUNTRY_FILTER_KEYS = {
    "alemanha",
    "austria",
    "belgica",
    "dinamarca",
    "espanha",
    "estados-unidos",
    "franca",
    "holanda",
    "italia",
    "luxemburgo",
    "noruega",
    "polonia",
    "portugal",
    "reino-unido",
    "russia",
    "suica",
    "suecia",
}
PORTUGAL_LOCATION_NAMES = {
    "Ilha da Madeira",
    "Ilha Terceira",
    "Leixoes",
    "Lisboa",
    "Porto",
    "Portugal",
    "Sao Miguel",
}
DEDICATED_ORIGIN_VIEWS = {
    "portugal": "portugal",
    "italia": "italy",
    "alemanha": "germany",
    "estados-unidos": "united_states",
}


def clean_text(value):
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def ascii_fold(text):
    normalized = unicodedata.normalize("NFKD", clean_text(text))
    return "".join(char for char in normalized if not unicodedata.combining(char))


def locality_key(value):
    folded = ascii_fold(value).lower()
    folded = re.sub(r"[^a-z0-9/ ]+", " ", folded)
    return clean_text(folded)


EUROPE_EXACT_ALIASES = {
    locality_key(alias): canonical for alias, canonical in EUROPE_RAW_EXACT_ALIASES.items()
}
EUROPE_PARTIAL_ALIASES = [
    (locality_key(alias), canonical) for alias, canonical in EUROPE_RAW_PARTIAL_ALIASES
]


def extract_year(value):
    match = re.search(r"(18\d{2}|19\d{2}|20\d{2})", clean_text(value))
    return int(match.group(1)) if match else None


def infer_arrival_year(row):
    for value in (row.get("arrival_date", ""), row.get("document_year", ""), row.get("departure_date", "")):
        year = extract_year(value)
        if year and VALID_YEAR_MIN <= year <= VALID_YEAR_MAX:
            return year
    return None


def infer_surname(row):
    surname = clean_text(row.get("surnames_norm", "")) or clean_text(row.get("surnames", ""))
    if surname:
        return surname

    person_name_raw = clean_text(row.get("person_name_raw", ""))
    if "," in person_name_raw:
        return clean_text(person_name_raw.split(",", 1)[0])
    return person_name_raw


def should_skip_point(row, surname):
    surname_key = re.sub(r"[^a-z]", "", ascii_fold(surname).lower())
    raw_name_key = ascii_fold(row.get("person_name_raw", "")).lower()
    return surname_key in IGNORED_SURNAME_KEYS or raw_name_key.startswith(("ass.", "ass:", "obs.", "obs:"))


def safe_value(*values):
    for value in values:
        cleaned = clean_text(value)
        if cleaned:
            return cleaned
    return ""


def is_brazil_origin(row):
    candidates = [
        row.get("origin_country_norm"),
        row.get("origin_country"),
        row.get("nationality_norm"),
        row.get("nationality"),
    ]
    for value in candidates:
        folded = ascii_fold(value).lower()
        if BRAZIL_PATTERN.search(folded):
            return True
    return False


def detect_country_filter_from_values(*values):
    candidates = [safe_value(value) for value in values]
    search_text = ascii_fold(" | ".join(value for value in candidates if value)).upper()
    if not search_text:
        return "", ""

    for keywords, key, label in COUNTRY_FILTER_RULES:
        if any(keyword in search_text for keyword in keywords):
            return key, label

    return "", ""


def detect_country_filter(row):
    candidates = [
        safe_value(row.get("origin_country_norm"), row.get("origin_country")),
        safe_value(row.get("nationality_norm"), row.get("nationality")),
        safe_value(row.get("birthplace_norm"), row.get("birthplace")),
    ]
    return detect_country_filter_from_values(*candidates)


def is_onomastic_index_record(row):
    return clean_text(row.get("document_type")) == "onomastic_index_entry"


def build_origin_metadata(row):
    origin_place = build_clean_origin_display(row)
    origin_country = safe_value(row.get("origin_country"), row.get("origin_country_norm"))
    nationality = safe_value(row.get("nationality"), row.get("nationality_norm"))
    _, detected_country_label = detect_country_filter(row)

    if is_onomastic_index_record(row):
        return {
            "origin_place_label": "Procedencia",
            "origin_place": origin_place or safe_value(row.get("origin_city"), row.get("birthplace")),
            "origin_country_label": "Nacionalidade" if nationality else "Pais inferido",
            "origin_country_display": nationality or detected_country_label or origin_country,
        }

    return {
        "origin_place_label": "Origem",
        "origin_place": origin_place,
        "origin_country_label": "Pais de origem",
        "origin_country_display": detected_country_label or origin_country or nationality,
    }


def looks_like_europe_noise(value):
    key = locality_key(value)
    if not key or key in {"?", "(?)"}:
        return True
    return any(fragment in key for fragment in EUROPE_NOISE_FRAGMENTS)


def looks_like_occupation_fragment(value):
    key = locality_key(value)
    if not key:
        return False
    return key in OCCUPATION_NOISE_KEYS


def split_origin_parts(value):
    return [clean_text(part) for part in clean_text(value).split(",") if clean_text(part)]


def expand_origin_candidates(*values):
    expanded = []
    seen_keys = set()

    for value in values:
        label = clean_text(value)
        if not label:
            continue

        parts = sanitize_origin_parts(split_origin_parts(label))
        candidate_values = list(parts)
        if label and not candidate_values:
            candidate_values.append(label)
        elif label and len(candidate_values) > 1:
            candidate_values.append(label)

        for candidate in candidate_values:
            key = locality_key(candidate)
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            expanded.append(candidate)

    return expanded


def looks_like_brazilian_location(value):
    key = locality_key(value)
    if not key:
        return False
    if BRAZIL_PATTERN.search(key):
        return True
    if any(hint in key for hint in BRAZIL_LOCATION_HINTS):
        return True
    return resolve_city(value) is not None


def sanitize_origin_parts(parts):
    cleaned_parts = [part for part in parts if part and not looks_like_europe_noise(part)]

    while cleaned_parts and looks_like_occupation_fragment(cleaned_parts[0]):
        cleaned_parts.pop(0)

    deduped_parts = []
    seen_keys = set()
    for part in cleaned_parts:
        key = locality_key(part)
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        deduped_parts.append(part)

    return deduped_parts


def alias_matches_tokens(value, alias):
    value_tokens = locality_key(value).split()
    alias_tokens = locality_key(alias).split()
    if not value_tokens or not alias_tokens:
        return False

    limit = len(value_tokens) - len(alias_tokens) + 1
    for start in range(max(limit, 0)):
        if all(value_tokens[start + index].startswith(token) for index, token in enumerate(alias_tokens)):
            return True
    return False


def build_clean_origin_display(row):
    return build_clean_origin_display_from_values(
        safe_value(row.get("birthplace"), row.get("birthplace_norm")),
        safe_value(row.get("origin_city"), row.get("origin_city_norm")),
        safe_value(row.get("origin_country"), row.get("origin_country_norm")),
    )


@lru_cache(maxsize=50000)
def build_clean_origin_display_from_values(birthplace_value, origin_city_value, origin_country_value):
    birthplace_parts = sanitize_origin_parts(
        split_origin_parts(birthplace_value)
    )
    origin_city = origin_city_value
    origin_country = origin_country_value
    clean_origin_city = (
        origin_city
        if origin_city and not looks_like_europe_noise(origin_city) and not looks_like_occupation_fragment(origin_city)
        else ""
    )

    if clean_origin_city:
        city_key = locality_key(clean_origin_city)
        if city_key and city_key not in {locality_key(part) for part in birthplace_parts}:
            birthplace_parts.insert(0, clean_origin_city)

    if birthplace_parts:
        return ", ".join(birthplace_parts)

    return safe_value(clean_origin_city, origin_country)


@lru_cache(maxsize=50000)
def resolve_europe_location_candidate(value):
    label = clean_text(value)
    if looks_like_europe_noise(label) or looks_like_occupation_fragment(label):
        return None

    key = locality_key(label)
    if key in EUROPE_EXACT_ALIASES:
        canonical_name = EUROPE_EXACT_ALIASES[key]
        location_meta = EUROPE_LOCATION_COORDS[canonical_name]
        return {
            "name": canonical_name,
            "state": location_meta["state"],
            "coords": location_meta["coords"],
            "matched_label": label,
        }

    for alias, canonical_name in EUROPE_PARTIAL_ALIASES:
        if alias_matches_tokens(key, alias):
            location_meta = EUROPE_LOCATION_COORDS[canonical_name]
            return {
                "name": canonical_name,
                "state": location_meta["state"],
                "coords": location_meta["coords"],
                "matched_label": label,
            }

    return None


def resolve_europe_origin(row):
    return resolve_europe_origin_from_values(
        build_clean_origin_display(row),
        safe_value(row.get("origin_city"), row.get("origin_city_norm")),
        safe_value(row.get("birthplace"), row.get("birthplace_norm")),
        safe_value(row.get("origin_country"), row.get("origin_country_norm")),
        safe_value(row.get("nationality"), row.get("nationality_norm")),
    )


@lru_cache(maxsize=100000)
def resolve_europe_origin_from_values(cleaned_origin, origin_city, birthplace, origin_country, nationality):
    candidates = expand_origin_candidates(
        cleaned_origin,
        origin_city,
        birthplace,
        origin_country,
        nationality,
    )

    for candidate in candidates:
        if looks_like_brazilian_location(candidate):
            continue
        resolved = resolve_europe_location_candidate(candidate)
        if resolved:
            return resolved

    _, detected_country_label = detect_country_filter_from_values(
        origin_country,
        nationality,
        birthplace,
        cleaned_origin,
    )
    if detected_country_label:
        return resolve_europe_location_candidate(detected_country_label)

    return None


def europe_country_label(location):
    _, label = detect_country_filter_from_values(location.get("name"), location.get("state"))
    return label or clean_text(location.get("state"))


def harmonize_europe_origin_display(cleaned_origin_display, europe_origin):
    resolved_label = safe_value(europe_origin.get("matched_label"), europe_origin.get("name"))
    if not cleaned_origin_display:
        return resolved_label

    parts = sanitize_origin_parts(split_origin_parts(cleaned_origin_display))
    if not parts:
        return resolved_label

    resolved_country = europe_country_label(europe_origin)
    kept_parts = []
    seen_keys = set()

    for part in parts:
        resolved_part = resolve_europe_location_candidate(part)
        if resolved_part:
            resolved_part_country = europe_country_label(resolved_part)
            if (
                resolved_part["name"] == europe_origin["name"]
                or resolved_part_country == resolved_country
            ):
                key = locality_key(part)
                if key and key not in seen_keys:
                    seen_keys.add(key)
                    kept_parts.append(part)
            continue

        _, part_country = detect_country_filter_from_values(part)
        if part_country:
            if part_country == resolved_country:
                key = locality_key(part)
                if key and key not in seen_keys:
                    seen_keys.add(key)
                    kept_parts.append(part)
            continue

        key = locality_key(part)
        if key == locality_key(resolved_label) and key not in seen_keys:
            seen_keys.add(key)
            kept_parts.append(part)

    return ", ".join(kept_parts) if kept_parts else resolved_label


def compose_origin_label(row):
    parts = [
        safe_value(row.get("origin_city")),
        safe_value(row.get("birthplace")),
        safe_value(row.get("origin_country")),
    ]
    unique_parts = []
    for part in parts:
        if part and part not in unique_parts:
            unique_parts.append(part)
    return ", ".join(unique_parts)


def compose_full_name(row, surname):
    given_names = safe_value(row.get("given_names"))
    if given_names and surname:
        return f"{given_names} {surname}"
    return safe_value(row.get("person_name_raw"), given_names, surname)


def combine_place_with_state(place, state):
    if place and state and place != state:
        return f"{place}, {state}"
    return place or state


def resolve_destination_source(row):
    return (
        clean_text(row.get("destination_locality"))
        or clean_text(row.get("arrival_port"))
        or safe_value(row.get("source_collection"), row.get("source"))
    )


def resolve_view_location_meta(view, place_name):
    view_mode = resolve_view_mode(view)
    if view_mode == "brazil" and place_name in CITY_COORDS:
        city_meta = CITY_COORDS[place_name]
        return {"coords": city_meta["coords"], "state": city_meta["state"]}
    if view_mode == "europe" and place_name in EUROPE_LOCATION_COORDS:
        location_meta = EUROPE_LOCATION_COORDS[place_name]
        return {"coords": location_meta["coords"], "state": location_meta["state"]}
    return {"coords": [0, 0], "state": ""}


def active_csv_path():
    if PROCESSED_CSV_PATH.exists():
        return PROCESSED_CSV_PATH
    return RAW_CSV_PATH


def database_url():
    return os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL).strip()


def postgres_enabled():
    return bool(database_url()) and psycopg is not None


def get_postgres_connection(row_factory=dict_row):
    return psycopg.connect(
        database_url(),
        row_factory=row_factory,
        connect_timeout=DB_CONNECT_TIMEOUT_SECONDS,
    )


def database_boot_message():
    return (
        "Banco Postgres nao esta pronto para as rotas de dados. "
        "Suba o banco com `docker compose up -d postgres`, "
        "recarregue a base com `python .\\scripts\\load_postgres_map_data.py` "
        "e depois rode `python app.py`."
    )


def postgres_ready():
    if not postgres_enabled():
        return False

    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      (
                      to_regclass('public.map_points') IS NOT NULL
                      AND to_regclass('public.map_view_stats') IS NOT NULL
                      AND to_regclass('public.map_build_meta') IS NOT NULL
                      ) AS ready
                    """
                )
                row = cur.fetchone()
                if not row:
                    return False
                if isinstance(row, dict):
                    return bool(row.get("ready"))
                return bool(row[0])
    except Exception:
        return False


def require_postgres_ready():
    if postgres_ready():
        return
    raise DatabaseNotReadyError(database_boot_message())


def resolve_view_mode(view):
    view_meta = MAP_CONFIG["views"].get(view) or MAP_CONFIG["views"][MAP_CONFIG["default_view"]]
    return clean_text(view_meta.get("point_view_mode")).lower() or "brazil"


def map_cache_path(view):
    csv_path = active_csv_path()
    if not csv_path.exists():
        return None
    stamp = csv_path.stat().st_mtime_ns
    return MAP_CACHE_DIR / f"{MAP_CACHE_VERSION}_{view}_{stamp}.pkl"


def load_cached_view(view):
    cache_path = map_cache_path(view)
    if cache_path is None or not cache_path.exists():
        return None

    try:
        with cache_path.open("rb") as handle:
            return pickle.load(handle)
    except Exception:
        return None


def store_cached_view(view, data):
    cache_path = map_cache_path(view)
    if cache_path is None:
        return

    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with cache_path.open("wb") as handle:
            pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        return


@lru_cache(maxsize=1)
def load_map_rows():
    csv_path = active_csv_path()
    if not csv_path.exists():
        return []

    frame = pd.read_csv(
        csv_path,
        dtype=str,
        usecols=lambda column: column in MAP_CSV_COLUMNS,
    ).fillna("")
    return frame.to_dict(orient="records")


@lru_cache(maxsize=20000)
def resolve_city(locality):
    key = locality_key(locality)
    if not key:
        return None

    if key in EXACT_LOCALITY_ALIASES:
        return EXACT_LOCALITY_ALIASES[key]

    matches = []
    for alias, city in PARTIAL_LOCALITY_ALIASES:
        alias_index = key.find(alias)
        if alias_index >= 0:
            matches.append((alias_index, -len(alias), city))

    if not matches:
        return None

    matches.sort()
    return matches[0][2]


def jitter_coords(coords, seed, slot_index=0, max_radius_km=18.0):
    digest = hashlib.sha1(seed.encode("utf-8")).digest()
    radius_ratio = int.from_bytes(digest[:4], "big") / 4294967295
    angle_ratio = int.from_bytes(digest[4:8], "big") / 4294967295
    radius_limit_km = min(max_radius_km, 0.55 + (0.22 * math.sqrt(slot_index + 1)))
    distance_km = radius_limit_km * math.sqrt(radius_ratio)
    angle = angle_ratio * math.tau
    latitude = coords[0]
    lat_offset = math.sin(angle) * distance_km / 111.0
    lon_base = max(math.cos(math.radians(latitude)), 0.35)
    lon_offset = math.cos(angle) * distance_km / (111.0 * lon_base)
    return [round(latitude + lat_offset, 6), round(coords[1] + lon_offset, 6)]


POINT_RESPONSE_LIMIT = 24000


def normalize_query_text(value):
    if not value:
        return ""

    return re.sub(r"\s+", " ", ascii_fold(value).upper()).strip()


def point_limit_for_view(view):
    if view == "south_brazil":
        return 12000
    if view == "southeast_brazil":
        return 12000
    if view == "portugal":
        return 7000
    if view == "italy":
        return 5000
    if view == "germany":
        return 7000
    if view == "united_states":
        return 4500
    if view == "europe_rest":
        return 5000
    return POINT_RESPONSE_LIMIT


def matches_brazil_view(view, state):
    if view == "south_brazil":
        return state in SOUTH_BRAZIL_STATES
    if view == "southeast_brazil":
        return state in SOUTHEAST_BRAZIL_STATES
    return False


def detect_europe_country(europe_origin, row):
    country_filter_key, country_filter_label = detect_country_filter_from_values(
        europe_origin["name"],
        europe_origin["state"],
    )
    if country_filter_key in EUROPE_COUNTRY_FILTER_KEYS:
        return country_filter_key, country_filter_label

    fallback_key, fallback_label = detect_country_filter(row)
    if fallback_key in EUROPE_COUNTRY_FILTER_KEYS:
        return fallback_key, fallback_label

    return country_filter_key, country_filter_label


def classify_europe_view(europe_origin, country_filter_key):
    if country_filter_key in DEDICATED_ORIGIN_VIEWS:
        return DEDICATED_ORIGIN_VIEWS[country_filter_key]
    if europe_origin["state"] == "Portugal" or europe_origin["name"] in PORTUGAL_LOCATION_NAMES:
        return "portugal"
    if country_filter_key in EUROPE_COUNTRY_FILTER_KEYS:
        return "europe_rest"
    return None


@lru_cache(maxsize=8)
def load_immigration_points(view="south_brazil"):
    if view not in MAP_CONFIG["views"]:
        view = MAP_CONFIG["default_view"]
    view_mode = resolve_view_mode(view)

    cached = load_cached_view(view)
    if cached is not None:
        return cached

    rows = load_map_rows()
    if not rows:
        return {
            "view": view,
            "points": [],
            "city_totals": [],
            "country_filters": [],
            "mapped_records": 0,
            "mapped_cities": 0,
            "year_min": None,
            "year_max": None,
            "unmapped_records": 0,
        }

    points = []
    city_totals = {}
    country_totals = {}
    unmapped_records = 0
    filtered_brazil_records = 0

    for index, row in enumerate(rows, start=1):
        year = infer_arrival_year(row)
        surname = infer_surname(row)

        if not year or not surname or should_skip_point(row, surname):
            unmapped_records += 1
            continue

        destination_source = clean_text(row.get("destination_locality")) or clean_text(row.get("arrival_port"))
        destination_fallback_source = resolve_destination_source(row)
        destination_city = resolve_city(destination_source)
        destination_display_city = resolve_city(destination_fallback_source)
        destination_display = (
            combine_place_with_state(
                destination_display_city,
                CITY_COORDS[destination_display_city]["state"],
            )
            if destination_display_city and destination_display_city in CITY_COORDS
            else destination_fallback_source
        )

        if view_mode == "brazil":
            canonical_city = destination_city
            if not canonical_city:
                unmapped_records += 1
                continue

            if is_brazil_origin(row):
                filtered_brazil_records += 1
                continue

            city_meta = CITY_COORDS.get(canonical_city)
            if not city_meta:
                unmapped_records += 1
                continue
            if not matches_brazil_view(view, city_meta["state"]):
                continue

            full_name = compose_full_name(row, surname)
            city_slot_index = city_totals.get(canonical_city, 0)
            coords = jitter_coords(
                city_meta["coords"],
                f"{view}:{clean_text(row.get('record_id')) or surname}:{index}",
                slot_index=city_slot_index,
                max_radius_km=14.0,
            )
            country_filter_key, country_filter_label = detect_country_filter(row)
            origin_metadata = build_origin_metadata(row)
            point = {
                "view_mode": "brazil",
                "view_key": view,
                "point_id": f"{view}:{clean_text(row.get('record_id')) or 'row'}:{index}",
                "full_name": full_name,
                "surname": surname,
                "surname_norm": safe_value(row.get("surnames_norm"), surname).upper(),
                "surname_search": normalize_query_text(safe_value(row.get("surnames_norm"), surname)),
                "full_name_search": normalize_query_text(full_name),
                "year": year,
                "city": canonical_city,
                "state": city_meta["state"],
                "locality_label": destination_source or canonical_city,
                "lat": coords[0],
                "lng": coords[1],
                "origin_place_label": origin_metadata["origin_place_label"],
                "origin_place": origin_metadata["origin_place"],
                "origin_country_label": origin_metadata["origin_country_label"],
                "origin_country_display": origin_metadata["origin_country_display"],
                "country_filter_key": country_filter_key,
                "country_filter_label": country_filter_label,
                "ship_name": safe_value(row.get("ship_name"), row.get("ship_name_norm")),
                "arrival_port": safe_value(row.get("arrival_port"), row.get("arrival_port_norm")),
                "destination_display": destination_display or combine_place_with_state(canonical_city, city_meta["state"]),
                "source": safe_value(row.get("source")),
                "source_collection": safe_value(row.get("source_collection")),
            }
        else:
            europe_origin = resolve_europe_origin(row)
            if not europe_origin:
                unmapped_records += 1
                continue

            cleaned_origin_display = build_clean_origin_display(row)
            europe_origin_display = ""
            if cleaned_origin_display and not looks_like_brazilian_location(cleaned_origin_display):
                europe_origin_display = harmonize_europe_origin_display(
                    cleaned_origin_display,
                    europe_origin,
                )
            country_filter_key, country_filter_label = detect_europe_country(europe_origin, row)
            europe_view_group = classify_europe_view(europe_origin, country_filter_key)
            if europe_view_group != view:
                continue
            full_name = compose_full_name(row, surname)
            city_slot_index = city_totals.get(europe_origin["name"], 0)
            coords = jitter_coords(
                europe_origin["coords"],
                f"{view}:{clean_text(row.get('record_id')) or surname}:{index}",
                slot_index=city_slot_index,
                max_radius_km=18.0,
            )
            point = {
                "view_mode": "europe",
                "view_key": view,
                "point_id": f"{view}:{clean_text(row.get('record_id')) or 'row'}:{index}",
                "full_name": full_name,
                "surname": surname,
                "surname_norm": safe_value(row.get("surnames_norm"), surname).upper(),
                "surname_search": normalize_query_text(safe_value(row.get("surnames_norm"), surname)),
                "full_name_search": normalize_query_text(full_name),
                "year": year,
                "city": europe_origin["name"],
                "state": europe_origin["state"],
                "locality_label": europe_origin_display or europe_origin["matched_label"] or europe_origin["name"],
                "lat": coords[0],
                "lng": coords[1],
                "origin_place_label": "Origem europeia",
                "origin_place": europe_origin_display or europe_origin["matched_label"] or europe_origin["name"],
                "origin_country_label": "Pais/Regiao",
                "origin_country_display": europe_origin["state"],
                "country_filter_key": country_filter_key,
                "country_filter_label": country_filter_label,
                "ship_name": safe_value(row.get("ship_name"), row.get("ship_name_norm")),
                "arrival_port": safe_value(row.get("arrival_port"), row.get("arrival_port_norm")),
                "destination_display": destination_display or safe_value(row.get("arrival_port"), row.get("arrival_port_norm")),
                "source": safe_value(row.get("source")),
                "source_collection": safe_value(row.get("source_collection")),
            }

        points.append(point)
        city_totals[point["city"]] = city_totals.get(point["city"], 0) + 1
        if country_filter_key:
            entry = country_totals.setdefault(
                country_filter_key,
                {"key": country_filter_key, "label": country_filter_label, "count": 0},
            )
            entry["count"] += 1

    points.sort(key=lambda item: (item["year"], item["city"], item["surname"], item["point_id"]))
    city_ranking = sorted(
        (
            {
                "city": city_name,
                "count": count,
                "coords": resolve_view_location_meta(view, city_name)["coords"],
                "state": resolve_view_location_meta(view, city_name)["state"],
            }
            for city_name, count in city_totals.items()
        ),
        key=lambda item: (-item["count"], item["city"]),
    )
    country_ranking = sorted(country_totals.values(), key=lambda item: (-item["count"], item["label"]))

    result = {
        "view": view,
        "points": points,
        "city_totals": city_ranking,
        "country_filters": country_ranking,
        "mapped_records": len(points),
        "mapped_cities": len(city_totals),
        "year_min": points[0]["year"] if points else None,
        "year_max": points[-1]["year"] if points else None,
        "unmapped_records": unmapped_records,
        "filtered_brazil_records": filtered_brazil_records,
    }
    store_cached_view(view, result)
    return result


POINT_SUMMARY_FIELDS = (
    "point_id",
    "view_mode",
    "view_key",
    "full_name",
    "surname",
    "year",
    "city",
    "state",
    "locality_label",
    "lat",
    "lng",
    "destination_display",
    "origin_place",
    "origin_country_display",
    "country_filter_key",
    "country_filter_label",
    "source",
    "source_collection",
)

DB_POINT_SUMMARY_SELECT = """
    point_id,
    view_mode,
    view_key,
    full_name,
    surname,
    year_num AS year,
    city,
    state,
    locality_label,
    lat,
    lng,
    destination_display,
    origin_place,
    origin_country_display,
    country_filter_key,
    country_filter_label,
    source,
    source_collection
"""

DB_POINT_DETAIL_SELECT = """
    point_id,
    view_mode,
    view_key,
    full_name,
    surname,
    surname_norm,
    surname_search,
    full_name_search,
    year_num AS year,
    city,
    state,
    locality_label,
    lat,
    lng,
    origin_place_label,
    origin_place,
    origin_country_label,
    origin_country_display,
    country_filter_key,
    country_filter_label,
    ship_name,
    arrival_port,
    destination_display,
    source,
    source_collection
"""


def serialize_point_summary(point):
    return {field: point.get(field) for field in POINT_SUMMARY_FIELDS}


@lru_cache(maxsize=8)
def build_point_lookup(view):
    data = load_immigration_points(view)
    return {point["point_id"]: point for point in data["points"]}


def point_matches_query(point, surname_query):
    return (
        not surname_query
        or surname_query in point.get("surname_search", "")
        or surname_query in point.get("full_name_search", "")
    )


def sample_points(points, limit):
    if len(points) <= limit:
        return points, False

    sampled = []
    step = len(points) / float(limit)
    cursor = 0.0
    for _ in range(limit):
        sampled.append(points[min(int(cursor), len(points) - 1)])
        cursor += step

    return sampled, True


def build_city_ranking(view, points):
    city_totals = {}
    for point in points:
        city_totals[point["city"]] = city_totals.get(point["city"], 0) + 1

    return sorted(
        (
            {
                "city": city_name,
                "count": count,
                "coords": resolve_view_location_meta(view, city_name)["coords"],
                "state": resolve_view_location_meta(view, city_name)["state"],
            }
            for city_name, count in city_totals.items()
        ),
        key=lambda item: (-item["count"], item["city"]),
    )


def build_surname_ranking(points):
    surname_totals = {}
    ignored_surnames = {"NAO CONSTA", "NADA CONSTA", "NAO INFORMADO", "IGNORADO"}
    for point in points:
        surname = clean_text(point.get("surname"))
        if not surname:
            continue
        if normalize_query_text(surname) in ignored_surnames:
            continue
        surname_totals[surname] = surname_totals.get(surname, 0) + 1

    return [
        {"surname": surname, "count": count}
        for surname, count in sorted(
            surname_totals.items(),
            key=lambda item: (-item[1], item[0]),
        )[:10]
    ]


def build_source_ranking(points):
    source_totals = {}
    for point in points:
        label = safe_value(point.get("source_collection"), point.get("source"))
        if not label:
            continue
        source_totals[label] = source_totals.get(label, 0) + 1

    return [
        {"label": label, "count": count}
        for label, count in sorted(
            source_totals.items(),
            key=lambda item: (-item[1], item[0]),
        )[:6]
    ]


def point_country_label(point):
    return safe_value(point.get("country_filter_label"))


def point_flow_origin(point):
    return safe_value(
        point.get("origin_country_display"),
        point.get("country_filter_label"),
        point.get("origin_place"),
        "Origem nao identificada",
    )


def point_flow_destination(point):
    return safe_value(
        point.get("destination_display"),
        combine_place_with_state(point.get("city"), point.get("state")),
        point.get("city"),
    )


def build_query_insights(points):
    year_totals = {}
    country_totals = {}
    city_totals = {}
    flow_totals = {}
    source_totals = {}
    country_keys = set()

    for point in points:
        year = point.get("year")
        if year:
            year_totals[year] = year_totals.get(year, 0) + 1

        city = clean_text(point.get("city"))
        if city:
            city_totals[city] = city_totals.get(city, 0) + 1

        country_label = point_country_label(point)
        if country_label:
            country_totals[country_label] = country_totals.get(country_label, 0) + 1
            country_keys.add(normalize_query_text(country_label))

        origin_label = point_flow_origin(point)
        destination_label = point_flow_destination(point)
        if origin_label and destination_label:
            flow_key = (origin_label, destination_label)
            flow_totals[flow_key] = flow_totals.get(flow_key, 0) + 1

        source_label = safe_value(point.get("source_collection"), point.get("source"))
        if source_label:
            source_totals[source_label] = source_totals.get(source_label, 0) + 1

    peak_year = max(year_totals.items(), key=lambda item: (item[1], -item[0])) if year_totals else None
    peak_country = max(country_totals.items(), key=lambda item: (item[1], item[0])) if country_totals else None
    peak_city = max(city_totals.items(), key=lambda item: (item[1], item[0])) if city_totals else None

    return {
        "source_totals": [
            {"label": label, "count": count}
            for label, count in sorted(
                source_totals.items(),
                key=lambda item: (-item[1], item[0]),
            )[:6]
        ],
        "top_flows": [
            {"origin": origin, "destination": destination, "count": count}
            for (origin, destination), count in sorted(
                flow_totals.items(),
                key=lambda item: (-item[1], item[0][0], item[0][1]),
            )[:10]
        ],
        "automatic_stats": {
            "peak_year": (
                {"label": peak_year[0], "count": peak_year[1]} if peak_year else None
            ),
            "peak_country": (
                {"label": peak_country[0], "count": peak_country[1]} if peak_country else None
            ),
            "peak_city": (
                {"label": peak_city[0], "count": peak_city[1]} if peak_city else None
            ),
            "country_count": len(country_keys),
        },
    }


def filter_query_points(data, year_max=None, surname_query="", country_keys=()):
    points = data["points"]
    min_year = data["year_min"]
    max_year = data["year_max"]
    if year_max is None:
        year_value = max_year
    else:
        year_value = max(min_year, min(int(year_max), max_year))

    normalized_query = normalize_query_text(surname_query)
    selected_countries = set(country_keys)
    points_before_country = []
    points_before_surname = []
    matched_points = []

    for point in points:
        if point["year"] > year_value:
            break
        if selected_countries and point.get("country_filter_key") not in selected_countries:
            if point_matches_query(point, normalized_query):
                points_before_country.append(point)
            continue
        points_before_surname.append(point)
        if not point_matches_query(point, normalized_query):
            continue
        points_before_country.append(point)
        matched_points.append(point)

    return {
        "query_year": year_value,
        "selected_countries": selected_countries,
        "points_before_country": points_before_country,
        "points_before_surname": points_before_surname,
        "matched_points": matched_points,
    }


def build_location_detail(city, points):
    selected_city = clean_text(city)
    city_points = [point for point in points if clean_text(point.get("city")) == selected_city]
    if not city_points:
        return None

    nationality_totals = {}
    period_totals = {}
    name_totals = {}
    records = []
    state = safe_value(city_points[0].get("state"))

    for point in city_points:
        country_label = point_country_label(point)
        if country_label:
            nationality_totals[country_label] = nationality_totals.get(country_label, 0) + 1

        year = point.get("year")
        if year:
            decade = (int(year) // 10) * 10
            period_totals[decade] = period_totals.get(decade, 0) + 1

        name_label = safe_value(point.get("full_name"), point.get("surname"))
        if name_label and normalize_query_text(name_label) not in {
            "NAO CONSTA",
            "NADA CONSTA",
            "NAO INFORMADO",
            "IGNORADO",
        }:
            name_totals[name_label] = name_totals.get(name_label, 0) + 1

        records.append(
            {
                "point_id": point.get("point_id"),
                "year": point.get("year"),
                "name": name_label,
                "origin": safe_value(point.get("origin_place"), point.get("origin_country_display")),
                "source": safe_value(point.get("source_collection"), point.get("source")),
            }
        )

    if not nationality_totals:
        for point in city_points:
            fallback_label = safe_value(point.get("origin_country_display"))
            if not fallback_label or len(fallback_label) > 40 or "," in fallback_label:
                continue
            nationality_totals[fallback_label] = nationality_totals.get(fallback_label, 0) + 1

    peak_period = None
    if period_totals:
        decade, count = max(period_totals.items(), key=lambda item: (item[1], -item[0]))
        peak_period = {"label": f"{decade}-{decade + 9}", "count": count}

    records.sort(key=lambda item: (item["year"], item["name"], item["source"]))

    return {
        "city": selected_city,
        "state": state,
        "place_label": combine_place_with_state(selected_city, state),
        "total_records": len(city_points),
        "top_nationalities": [
            {"label": label, "count": count}
            for label, count in sorted(
                nationality_totals.items(),
                key=lambda item: (-item[1], item[0]),
            )[:5]
        ],
        "peak_period": peak_period,
        "top_names": [
            {"label": label, "count": count}
            for label, count in sorted(
                name_totals.items(),
                key=lambda item: (-item[1], item[0]),
            )[:8]
        ],
        "records": records[:30],
        "records_total": len(records),
    }


def build_country_filter_response(available_filters, points, selected_keys):
    counts = {}
    for point in points:
        key = point.get("country_filter_key")
        if not key:
            continue
        counts[key] = counts.get(key, 0) + 1

    filters = []
    for filter_meta in available_filters:
        count = counts.get(filter_meta["key"], 0)
        if count or filter_meta["key"] in selected_keys:
            filters.append({**filter_meta, "count": count})

    return sorted(filters, key=lambda item: (-item["count"], item["label"]))


def _query_immigration_points_uncached(
    view,
    year_max=None,
    surname_query="",
    country_keys=(),
    load_all=False,
):
    data = load_immigration_points(view)
    sampling_limit = point_limit_for_view(view)
    if not data["points"]:
        return {
            **data,
            "query_year": None,
            "matched_records": 0,
            "rendered_records": 0,
            "matched_places": 0,
            "surname_totals": [],
            "source_totals": [],
            "top_flows": [],
            "automatic_stats": {},
            "sampling_applied": False,
            "sampling_limit": sampling_limit,
            "load_all": load_all,
        }

    filter_result = filter_query_points(
        data,
        year_max=year_max,
        surname_query=surname_query,
        country_keys=country_keys,
    )
    matched_points = filter_result["matched_points"]
    points_before_country = filter_result["points_before_country"]
    points_before_surname = filter_result["points_before_surname"]
    selected_countries = filter_result["selected_countries"]

    if load_all:
        rendered_points = matched_points
        sampling_applied = False
    else:
        rendered_points, sampling_applied = sample_points(matched_points, sampling_limit)
    insights = build_query_insights(matched_points)

    return {
        "view": view,
        "points": [serialize_point_summary(point) for point in rendered_points],
        "city_totals": build_city_ranking(view, matched_points),
        "surname_totals": build_surname_ranking(points_before_surname),
        "source_totals": insights["source_totals"],
        "top_flows": insights["top_flows"],
        "automatic_stats": insights["automatic_stats"],
        "country_filters": build_country_filter_response(
            data["country_filters"], points_before_country, selected_countries
        ),
        "mapped_records": data["mapped_records"],
        "mapped_cities": data["mapped_cities"],
        "year_min": data["year_min"],
        "year_max": data["year_max"],
        "query_year": filter_result["query_year"],
        "matched_records": len(matched_points),
        "rendered_records": len(rendered_points),
        "matched_places": len({point["city"] for point in matched_points}),
        "sampling_applied": sampling_applied,
        "sampling_limit": sampling_limit,
        "load_all": load_all,
        "unmapped_records": data["unmapped_records"],
        "filtered_brazil_records": data.get("filtered_brazil_records", 0),
    }


def current_data_stamp():
    require_postgres_ready()
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(MAX(source_mtime_ns), 0) AS stamp
                FROM map_build_meta
                """
            )
            row = cur.fetchone()
            if not row:
                return 0
            if isinstance(row, dict):
                value = row.get("stamp")
            else:
                value = row[0]
            return int(value) if value is not None else 0


def empty_view_payload(view, load_all=False):
    return {
        "view": view,
        "points": [],
        "city_totals": [],
        "surname_totals": [],
        "source_totals": [],
        "top_flows": [],
        "automatic_stats": {},
        "country_filters": [],
        "mapped_records": 0,
        "mapped_cities": 0,
        "year_min": None,
        "year_max": None,
        "query_year": None,
        "matched_records": 0,
        "rendered_records": 0,
        "matched_places": 0,
        "sampling_applied": False,
        "sampling_limit": point_limit_for_view(view),
        "load_all": load_all,
        "unmapped_records": 0,
        "filtered_brazil_records": 0,
    }


@lru_cache(maxsize=32)
def load_db_view_stats(data_stamp, view):
    require_postgres_ready()
    stats = {
        "view": view,
        "mapped_records": 0,
        "mapped_cities": 0,
        "year_min": None,
        "year_max": None,
        "unmapped_records": 0,
        "filtered_brazil_records": 0,
    }
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  view_key,
                  mapped_records,
                  mapped_cities,
                  year_min,
                  year_max,
                  unmapped_records,
                  filtered_brazil_records
                FROM map_view_stats
                WHERE view_key = %s
                """,
                (view,),
            )
            row = cur.fetchone()
            if not row:
                return stats
            stats.update(
                {
                    "view": row["view_key"],
                    "mapped_records": int(row["mapped_records"] or 0),
                    "mapped_cities": int(row["mapped_cities"] or 0),
                    "year_min": int(row["year_min"]) if row["year_min"] is not None else None,
                    "year_max": int(row["year_max"]) if row["year_max"] is not None else None,
                    "unmapped_records": int(row["unmapped_records"] or 0),
                    "filtered_brazil_records": int(row["filtered_brazil_records"] or 0),
                }
            )
            return stats


@lru_cache(maxsize=32)
def load_db_country_catalog(data_stamp, view):
    require_postgres_ready()
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  country_filter_key AS key,
                  MAX(country_filter_label) AS label
                FROM map_points
                WHERE view_key = %s
                  AND country_filter_key <> ''
                GROUP BY country_filter_key
                ORDER BY label
                """,
                (view,),
            )
            return [
                {
                    "key": clean_text(row["key"]).lower(),
                    "label": clean_text(row["label"]),
                }
                for row in cur.fetchall()
                if clean_text(row["key"])
            ]


def clamp_query_year(stats, year_max):
    min_year = stats.get("year_min")
    max_year = stats.get("year_max")
    if min_year is None or max_year is None:
        return None
    if year_max is None:
        return max_year
    return max(min_year, min(int(year_max), max_year))


def build_db_filter_sql(
    view,
    year_value,
    surname_query="",
    country_keys=(),
    apply_country=True,
    apply_surname=True,
):
    clauses = ["view_key = %s", "year_num <= %s"]
    params = [view, int(year_value)]

    if apply_country and country_keys:
        clauses.append("country_filter_key = ANY(%s)")
        params.append(list(country_keys))

    if apply_surname and surname_query:
        pattern = f"%{normalize_query_text(surname_query)}%"
        clauses.append("(surname_search LIKE %s OR full_name_search LIKE %s)")
        params.extend([pattern, pattern])

    return " AND ".join(clauses), params


def build_db_country_filter_response(conn, data_stamp, view, year_value, surname_query, selected_keys):
    counts = {}
    where_sql, params = build_db_filter_sql(
        view,
        year_value,
        surname_query=surname_query,
        country_keys=(),
        apply_country=False,
        apply_surname=True,
    )

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
              country_filter_key AS key,
              MAX(country_filter_label) AS label,
              COUNT(*)::int AS count
            FROM map_points
            WHERE {where_sql}
              AND country_filter_key <> ''
            GROUP BY country_filter_key
            ORDER BY count DESC, label
            """,
            params,
        )
        for row in cur.fetchall():
            key = clean_text(row["key"]).lower()
            if not key:
                continue
            counts[key] = {
                "key": key,
                "label": clean_text(row["label"]),
                "count": int(row["count"] or 0),
            }

    filters = []
    for item in load_db_country_catalog(data_stamp, view):
        count = counts.get(item["key"], {}).get("count", 0)
        if count or item["key"] in selected_keys:
            filters.append(
                {
                    "key": item["key"],
                    "label": item["label"],
                    "count": count,
                }
            )

    return sorted(filters, key=lambda item: (-item["count"], item["label"]))


def query_db_points(
    conn,
    view,
    year_value,
    surname_query="",
    country_keys=(),
    load_all=False,
):
    where_sql, params = build_db_filter_sql(
        view,
        year_value,
        surname_query=surname_query,
        country_keys=country_keys,
        apply_country=True,
        apply_surname=True,
    )

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*)::int AS total FROM map_points WHERE {where_sql}",
            params,
        )
        matched_records = int(cur.fetchone()["total"] or 0)

        cur.execute(
            f"SELECT COUNT(DISTINCT city)::int AS total FROM map_points WHERE {where_sql}",
            params,
        )
        matched_places = int(cur.fetchone()["total"] or 0)

        sampling_limit = point_limit_for_view(view)
        sampling_applied = (not load_all) and matched_records > sampling_limit

        if load_all or not sampling_applied:
            cur.execute(
                f"""
                SELECT {DB_POINT_SUMMARY_SELECT}
                FROM map_points
                WHERE {where_sql}
                ORDER BY year_num, city, surname, point_id
                """,
                params,
            )
            points = cur.fetchall()
        else:
            sample_params = params + [matched_records, sampling_limit, sampling_limit, matched_records]
            cur.execute(
                f"""
                WITH ordered AS (
                  SELECT
                    {DB_POINT_SUMMARY_SELECT},
                    row_number() OVER (ORDER BY year_num, city, surname, point_id) AS rn
                  FROM map_points
                  WHERE {where_sql}
                ),
                bucketed AS (
                  SELECT
                    *,
                    CASE
                      WHEN %s <= %s THEN rn
                      ELSE floor(((rn - 1)::numeric * %s) / %s)
                    END AS bucket
                  FROM ordered
                )
                SELECT
                  point_id,
                  view_mode,
                  view_key,
                  full_name,
                  surname,
                  year,
                  city,
                  state,
                  locality_label,
                  lat,
                  lng,
                  destination_display,
                  origin_place,
                  origin_country_display,
                  country_filter_key,
                  country_filter_label,
                  source,
                  source_collection
                FROM (
                  SELECT DISTINCT ON (bucket)
                    bucket,
                    rn,
                    point_id,
                    view_mode,
                    view_key,
                    full_name,
                    surname,
                    year,
                    city,
                    state,
                    locality_label,
                    lat,
                    lng,
                    destination_display,
                    origin_place,
                    origin_country_display,
                    country_filter_key,
                    country_filter_label,
                    source,
                    source_collection
                  FROM bucketed
                  ORDER BY bucket, rn
                ) sampled
                ORDER BY year, city, surname, point_id
                """,
                sample_params,
            )
            points = cur.fetchall()

    return (
        [
            {
                "point_id": row["point_id"],
                "view_mode": row["view_mode"],
                "view_key": row["view_key"],
                "full_name": row["full_name"],
                "surname": row["surname"],
                "year": int(row["year"]) if row["year"] is not None else None,
                "city": row["city"],
                "state": row["state"],
                "locality_label": row["locality_label"],
                "lat": float(row["lat"]),
                "lng": float(row["lng"]),
                "destination_display": row["destination_display"],
                "origin_place": row["origin_place"],
                "origin_country_display": row["origin_country_display"],
                "country_filter_key": row["country_filter_key"],
                "country_filter_label": row["country_filter_label"],
                "source": row["source"],
                "source_collection": row["source_collection"],
            }
            for row in points
        ],
        matched_records,
        matched_places,
        sampling_applied,
        sampling_limit,
    )


def query_db_city_ranking(conn, view, year_value, surname_query="", country_keys=()):
    where_sql, params = build_db_filter_sql(
        view,
        year_value,
        surname_query=surname_query,
        country_keys=country_keys,
        apply_country=True,
        apply_surname=True,
    )

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
              city,
              MAX(state) AS state,
              COUNT(*)::int AS count
            FROM map_points
            WHERE {where_sql}
            GROUP BY city
            ORDER BY count DESC, city
            """,
            params,
        )
        rows = cur.fetchall()

    ranking = []
    for row in rows:
        location_meta = resolve_view_location_meta(view, row["city"])
        ranking.append(
            {
                "city": row["city"],
                "count": int(row["count"] or 0),
                "coords": location_meta["coords"],
                "state": row["state"] or location_meta["state"],
            }
        )
    return ranking


def query_db_surname_ranking(conn, view, year_value, country_keys=()):
    ignored_surnames = ("NAO CONSTA", "NADA CONSTA", "NAO INFORMADO", "IGNORADO")
    where_sql, params = build_db_filter_sql(
        view,
        year_value,
        surname_query="",
        country_keys=country_keys,
        apply_country=True,
        apply_surname=False,
    )

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
              surname,
              COUNT(*)::int AS count
            FROM map_points
            WHERE {where_sql}
              AND surname <> ''
              AND surname_search <> ALL(%s)
            GROUP BY surname
            ORDER BY count DESC, surname
            LIMIT 10
            """,
            params + [list(ignored_surnames)],
        )
        return [
            {"surname": row["surname"], "count": int(row["count"] or 0)}
            for row in cur.fetchall()
        ]


def query_db_source_ranking(conn, view, year_value, surname_query="", country_keys=()):
    where_sql, params = build_db_filter_sql(
        view,
        year_value,
        surname_query=surname_query,
        country_keys=country_keys,
        apply_country=True,
        apply_surname=True,
    )

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
              COALESCE(NULLIF(source_collection, ''), source) AS label,
              COUNT(*)::int AS count
            FROM map_points
            WHERE {where_sql}
              AND COALESCE(NULLIF(source_collection, ''), source) <> ''
            GROUP BY label
            ORDER BY count DESC, label
            LIMIT 6
            """,
            params,
        )
        return [
            {"label": row["label"], "count": int(row["count"] or 0)}
            for row in cur.fetchall()
        ]


def query_db_flow_ranking(conn, view, year_value, surname_query="", country_keys=()):
    where_sql, params = build_db_filter_sql(
        view,
        year_value,
        surname_query=surname_query,
        country_keys=country_keys,
        apply_country=True,
        apply_surname=True,
    )

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
              COALESCE(
                NULLIF(origin_country_display, ''),
                NULLIF(country_filter_label, ''),
                NULLIF(origin_place, ''),
                'Origem nao identificada'
              ) AS origin,
              COALESCE(
                NULLIF(destination_display, ''),
                NULLIF(
                  CASE
                    WHEN city <> '' AND state <> '' AND city <> state THEN city || ', ' || state
                    WHEN city <> '' THEN city
                    ELSE state
                  END,
                  ''
                ),
                city,
                'Destino nao identificado'
              ) AS destination,
              COUNT(*)::int AS count
            FROM map_points
            WHERE {where_sql}
            GROUP BY origin, destination
            ORDER BY count DESC, origin, destination
            LIMIT 10
            """,
            params,
        )
        return [
            {
                "origin": row["origin"],
                "destination": row["destination"],
                "count": int(row["count"] or 0),
            }
            for row in cur.fetchall()
        ]


def query_db_automatic_stats(conn, view, year_value, surname_query="", country_keys=()):
    where_sql, params = build_db_filter_sql(
        view,
        year_value,
        surname_query=surname_query,
        country_keys=country_keys,
        apply_country=True,
        apply_surname=True,
    )

    stats = {
        "peak_year": None,
        "peak_country": None,
        "peak_city": None,
        "country_count": 0,
    }

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT year_num AS label, COUNT(*)::int AS count
            FROM map_points
            WHERE {where_sql}
            GROUP BY year_num
            ORDER BY count DESC, label ASC
            LIMIT 1
            """,
            params,
        )
        row = cur.fetchone()
        if row:
            stats["peak_year"] = {"label": int(row["label"]), "count": int(row["count"] or 0)}

        cur.execute(
            f"""
            SELECT country_filter_label AS label, COUNT(*)::int AS count
            FROM map_points
            WHERE {where_sql}
              AND country_filter_label <> ''
            GROUP BY country_filter_label
            ORDER BY count DESC, label DESC
            LIMIT 1
            """,
            params,
        )
        row = cur.fetchone()
        if row:
            stats["peak_country"] = {"label": row["label"], "count": int(row["count"] or 0)}

        cur.execute(
            f"""
            SELECT city AS label, COUNT(*)::int AS count
            FROM map_points
            WHERE {where_sql}
            GROUP BY city
            ORDER BY count DESC, label DESC
            LIMIT 1
            """,
            params,
        )
        row = cur.fetchone()
        if row:
            stats["peak_city"] = {"label": row["label"], "count": int(row["count"] or 0)}

        cur.execute(
            f"""
            SELECT COUNT(DISTINCT UPPER(country_filter_label))::int AS total
            FROM map_points
            WHERE {where_sql}
              AND country_filter_label <> ''
            """,
            params,
        )
        row = cur.fetchone()
        stats["country_count"] = int(row["total"] or 0) if row else 0

    return stats


def _query_immigration_points_db(
    data_stamp,
    view,
    year_max=None,
    surname_query="",
    country_keys=(),
    load_all=False,
):
    stats = load_db_view_stats(data_stamp, view)
    if not stats["mapped_records"]:
        return empty_view_payload(view, load_all=load_all)

    year_value = clamp_query_year(stats, year_max)
    if year_value is None:
        return empty_view_payload(view, load_all=load_all)

    with get_postgres_connection() as conn:
        points, matched_records, matched_places, sampling_applied, sampling_limit = query_db_points(
            conn,
            view,
            year_value,
            surname_query=surname_query,
            country_keys=country_keys,
            load_all=load_all,
        )

        return {
            "view": view,
            "points": points,
            "city_totals": query_db_city_ranking(
                conn,
                view,
                year_value,
                surname_query=surname_query,
                country_keys=country_keys,
            ),
            "surname_totals": query_db_surname_ranking(
                conn,
                view,
                year_value,
                country_keys=country_keys,
            ),
            "source_totals": query_db_source_ranking(
                conn,
                view,
                year_value,
                surname_query=surname_query,
                country_keys=country_keys,
            ),
            "top_flows": query_db_flow_ranking(
                conn,
                view,
                year_value,
                surname_query=surname_query,
                country_keys=country_keys,
            ),
            "automatic_stats": query_db_automatic_stats(
                conn,
                view,
                year_value,
                surname_query=surname_query,
                country_keys=country_keys,
            ),
            "country_filters": build_db_country_filter_response(
                conn,
                data_stamp,
                view,
                year_value,
                surname_query,
                set(country_keys),
            ),
            "mapped_records": stats["mapped_records"],
            "mapped_cities": stats["mapped_cities"],
            "year_min": stats["year_min"],
            "year_max": stats["year_max"],
            "query_year": year_value,
            "matched_records": matched_records,
            "rendered_records": len(points),
            "matched_places": matched_places,
            "sampling_applied": sampling_applied,
            "sampling_limit": sampling_limit,
            "load_all": load_all,
            "unmapped_records": stats["unmapped_records"],
            "filtered_brazil_records": stats["filtered_brazil_records"],
        }


def _query_point_details_db(data_stamp, view, point_id):
    require_postgres_ready()
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {DB_POINT_DETAIL_SELECT}
                FROM map_points
                WHERE view_key = %s
                  AND point_id = %s
                LIMIT 1
                """,
                (view, point_id),
            )
            row = cur.fetchone()
            if not row:
                return None
            point = dict(row)
            if point.get("year") is not None:
                point["year"] = int(point["year"])
            if point.get("lat") is not None:
                point["lat"] = float(point["lat"])
            if point.get("lng") is not None:
                point["lng"] = float(point["lng"])
            return point


def _query_location_details_db(data_stamp, view, city, year_max=None, surname_query="", country_keys=()):
    stats = load_db_view_stats(data_stamp, view)
    if not stats["mapped_records"]:
        return None

    year_value = clamp_query_year(stats, year_max)
    if year_value is None:
        return None

    normalized_city = clean_text(city)
    if not normalized_city:
        return None

    where_sql, params = build_db_filter_sql(
        view,
        year_value,
        surname_query=surname_query,
        country_keys=country_keys,
        apply_country=True,
        apply_surname=True,
    )
    city_where_sql = f"{where_sql} AND city = %s"
    city_params = params + [normalized_city]

    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                  COUNT(*)::int AS total_records,
                  MAX(state) AS state
                FROM map_points
                WHERE {city_where_sql}
                """,
                city_params,
            )
            summary = cur.fetchone()
            total_records = int(summary["total_records"] or 0) if summary else 0
            if not total_records:
                return None

            state = clean_text(summary["state"])

            cur.execute(
                f"""
                SELECT
                  country_filter_label AS label,
                  COUNT(*)::int AS count
                FROM map_points
                WHERE {city_where_sql}
                  AND country_filter_label <> ''
                GROUP BY country_filter_label
                ORDER BY count DESC, label
                LIMIT 5
                """,
                city_params,
            )
            top_nationalities = [
                {"label": row["label"], "count": int(row["count"] or 0)}
                for row in cur.fetchall()
            ]

            if not top_nationalities:
                cur.execute(
                    f"""
                    SELECT
                      origin_country_display AS label,
                      COUNT(*)::int AS count
                    FROM map_points
                    WHERE {city_where_sql}
                      AND origin_country_display <> ''
                      AND LENGTH(origin_country_display) <= 40
                      AND POSITION(',' IN origin_country_display) = 0
                    GROUP BY origin_country_display
                    ORDER BY count DESC, label
                    LIMIT 5
                    """,
                    city_params,
                )
                top_nationalities = [
                    {"label": row["label"], "count": int(row["count"] or 0)}
                    for row in cur.fetchall()
                ]

            cur.execute(
                f"""
                SELECT
                  (year_num / 10) * 10 AS decade,
                  COUNT(*)::int AS count
                FROM map_points
                WHERE {city_where_sql}
                GROUP BY decade
                ORDER BY count DESC, decade ASC
                LIMIT 1
                """,
                city_params,
            )
            row = cur.fetchone()
            peak_period = None
            if row and row["decade"] is not None:
                decade = int(row["decade"])
                peak_period = {
                    "label": f"{decade}-{decade + 9}",
                    "count": int(row["count"] or 0),
                }

            cur.execute(
                f"""
                SELECT
                  COALESCE(NULLIF(full_name, ''), surname) AS label,
                  COUNT(*)::int AS count
                FROM map_points
                WHERE {city_where_sql}
                  AND UPPER(COALESCE(NULLIF(full_name, ''), surname)) <> ALL(%s)
                GROUP BY label
                ORDER BY count DESC, label
                LIMIT 8
                """,
                city_params + [["NAO CONSTA", "NADA CONSTA", "NAO INFORMADO", "IGNORADO"]],
            )
            top_names = [
                {"label": row["label"], "count": int(row["count"] or 0)}
                for row in cur.fetchall()
            ]

            cur.execute(
                f"""
                SELECT
                  point_id,
                  year_num AS year,
                  COALESCE(NULLIF(full_name, ''), surname) AS name,
                  COALESCE(NULLIF(origin_place, ''), origin_country_display) AS origin,
                  COALESCE(NULLIF(source_collection, ''), source) AS source
                FROM map_points
                WHERE {city_where_sql}
                ORDER BY year_num, name, source
                LIMIT 30
                """,
                city_params,
            )
            records = [
                {
                    "point_id": row["point_id"],
                    "year": int(row["year"]) if row["year"] is not None else None,
                    "name": row["name"],
                    "origin": row["origin"] or "",
                    "source": row["source"] or "",
                }
                for row in cur.fetchall()
            ]

    return {
        "city": normalized_city,
        "state": state,
        "place_label": combine_place_with_state(normalized_city, state),
        "total_records": total_records,
        "top_nationalities": top_nationalities,
        "peak_period": peak_period,
        "top_names": top_names,
        "records": records,
        "records_total": total_records,
    }


@lru_cache(maxsize=64)
def _cached_query_immigration_points(data_stamp, view, year_max, surname_query, country_keys):
    return _query_immigration_points_uncached(
        view=view,
        year_max=year_max,
        surname_query=surname_query,
        country_keys=country_keys,
    )


@lru_cache(maxsize=32)
def _cached_query_immigration_points_load_all(
    data_stamp,
    view,
    year_max,
    surname_query,
    country_keys,
):
    return _query_immigration_points_uncached(
        view=view,
        year_max=year_max,
        surname_query=surname_query,
        country_keys=country_keys,
        load_all=True,
    )


@lru_cache(maxsize=64)
def _cached_query_immigration_points_db(
    data_stamp,
    view,
    year_max,
    surname_query,
    country_keys,
):
    return _query_immigration_points_db(
        data_stamp=data_stamp,
        view=view,
        year_max=year_max,
        surname_query=surname_query,
        country_keys=country_keys,
        load_all=False,
    )


@lru_cache(maxsize=32)
def _cached_query_immigration_points_db_load_all(
    data_stamp,
    view,
    year_max,
    surname_query,
    country_keys,
):
    return _query_immigration_points_db(
        data_stamp=data_stamp,
        view=view,
        year_max=year_max,
        surname_query=surname_query,
        country_keys=country_keys,
        load_all=True,
    )


def query_immigration_points(view, year_max=None, surname_query="", country_keys=(), load_all=False):
    require_postgres_ready()
    normalized_view = clean_text(view).lower() or MAP_CONFIG["default_view"]
    normalized_year = int(year_max) if year_max is not None else None
    normalized_surname = clean_text(surname_query)
    normalized_country_keys = tuple(sorted(clean_text(value).lower() for value in country_keys if clean_text(value)))
    if load_all:
        return _cached_query_immigration_points_db_load_all(
            current_data_stamp(),
            normalized_view,
            normalized_year,
            normalized_surname,
            normalized_country_keys,
        )
    return _cached_query_immigration_points_db(
        current_data_stamp(),
        normalized_view,
        normalized_year,
        normalized_surname,
        normalized_country_keys,
    )


@lru_cache(maxsize=128)
def _cached_query_location_details(data_stamp, view, city, year_max=None, surname_query="", country_keys=()):
    data = load_immigration_points(view)
    if not data["points"]:
        return None

    filter_result = filter_query_points(
        data,
        year_max=year_max,
        surname_query=surname_query,
        country_keys=country_keys,
    )
    return build_location_detail(city, filter_result["matched_points"])


@lru_cache(maxsize=128)
def _cached_query_location_details_db(
    data_stamp,
    view,
    city,
    year_max=None,
    surname_query="",
    country_keys=(),
):
    return _query_location_details_db(
        data_stamp=data_stamp,
        view=view,
        city=city,
        year_max=year_max,
        surname_query=surname_query,
        country_keys=country_keys,
    )


@lru_cache(maxsize=20000)
def _cached_query_point_details_db(data_stamp, view, point_id):
    return _query_point_details_db(
        data_stamp=data_stamp,
        view=view,
        point_id=point_id,
    )


def query_location_details(view, city, year_max=None, surname_query="", country_keys=()):
    require_postgres_ready()
    normalized_view = clean_text(view).lower() or MAP_CONFIG["default_view"]
    normalized_city = clean_text(city)
    normalized_year = int(year_max) if year_max is not None else None
    normalized_surname = clean_text(surname_query)
    normalized_country_keys = tuple(sorted(clean_text(value).lower() for value in country_keys if clean_text(value)))
    return _cached_query_location_details_db(
        current_data_stamp(),
        normalized_view,
        normalized_city,
        normalized_year,
        normalized_surname,
        normalized_country_keys,
    )


def parse_request_filters():
    view = clean_text(request.args.get("view")).lower() or MAP_CONFIG["default_view"]
    year_max_raw = clean_text(request.args.get("year_max"))
    year_max = int(year_max_raw) if year_max_raw.isdigit() else None
    surname_query = clean_text(request.args.get("surname"))
    load_all = clean_text(request.args.get("load_all")).lower() in {"1", "true", "yes", "on"}
    country_keys = tuple(
        sorted(
            {
                clean_text(value).lower()
                for value in clean_text(request.args.get("countries")).split(",")
                if clean_text(value)
            }
        )
    )
    return view, year_max, surname_query, country_keys, load_all


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/healthz")
def healthz():
    return jsonify(
        {
            "status": "ok",
            "database_ready": postgres_ready(),
            "database_url_present": bool(database_url()),
        }
    )


@app.errorhandler(DatabaseNotReadyError)
def handle_database_not_ready(error):
    message = str(error) or database_boot_message()
    if request.path.startswith("/api/"):
        return jsonify({"error": message}), 503
    return message, 503


@app.get("/api/map-config")
def map_config():
    return jsonify(MAP_CONFIG)


@app.get("/api/immigration-points")
def immigration_points():
    view, year_max, surname_query, country_keys, load_all = parse_request_filters()
    if view not in MAP_CONFIG["views"]:
        return jsonify({"error": "view invalida"}), 400
    data = query_immigration_points(
        view=view,
        year_max=year_max,
        surname_query=surname_query,
        country_keys=country_keys,
        load_all=load_all,
    )
    return jsonify(data)


@app.get("/api/point-details")
def point_details():
    view = clean_text(request.args.get("view")).lower() or MAP_CONFIG["default_view"]
    if view not in MAP_CONFIG["views"]:
        return jsonify({"error": "view invalida"}), 400

    point_id = clean_text(request.args.get("point_id"))
    if not point_id:
        return jsonify({"error": "point_id obrigatorio"}), 400

    require_postgres_ready()
    point = _cached_query_point_details_db(current_data_stamp(), view, point_id)
    if not point:
        return jsonify({"error": "ponto nao encontrado"}), 404

    return jsonify(point)


@app.get("/api/location-details")
def location_details():
    view, year_max, surname_query, country_keys, _load_all = parse_request_filters()
    if view not in MAP_CONFIG["views"]:
        return jsonify({"error": "view invalida"}), 400

    city = clean_text(request.args.get("city"))
    if not city:
        return jsonify({"error": "city obrigatoria"}), 400

    detail = query_location_details(
        view=view,
        city=city,
        year_max=year_max,
        surname_query=surname_query,
        country_keys=country_keys,
    )
    if not detail:
        return jsonify({"error": "localidade nao encontrada no recorte atual"}), 404

    return jsonify(detail)


if __name__ == "__main__":
    if postgres_ready():
        print(f"Usando Postgres em {database_url()}")
    else:
        print(f"Aviso: {database_boot_message()}")
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        debug=os.getenv("FLASK_DEBUG", "0") == "1",
    )
