import argparse
import csv
import sys
from pathlib import Path

try:
    from psycopg import connect, sql
except Exception:  # pragma: no cover - dependencia opcional durante setup
    connect = None
    sql = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import DEFAULT_DATABASE_URL, MAP_CONFIG, active_csv_path, load_immigration_points

MAP_POINT_COLUMNS = [
    ("point_id", "text PRIMARY KEY"),
    ("view_mode", "text NOT NULL"),
    ("view_key", "text NOT NULL"),
    ("full_name", "text"),
    ("surname", "text"),
    ("surname_norm", "text"),
    ("surname_search", "text"),
    ("full_name_search", "text"),
    ("year_num", "integer NOT NULL"),
    ("city", "text NOT NULL"),
    ("state", "text"),
    ("locality_label", "text"),
    ("lat", "double precision NOT NULL"),
    ("lng", "double precision NOT NULL"),
    ("origin_place_label", "text"),
    ("origin_place", "text"),
    ("origin_country_label", "text"),
    ("origin_country_display", "text"),
    ("country_filter_key", "text"),
    ("country_filter_label", "text"),
    ("ship_name", "text"),
    ("arrival_port", "text"),
    ("destination_display", "text"),
    ("source", "text"),
    ("source_collection", "text"),
]

MAP_VIEW_STATS_INSERT_SQL = """
    INSERT INTO map_view_stats (
      view_key,
      mapped_records,
      mapped_cities,
      year_min,
      year_max,
      unmapped_records,
      filtered_brazil_records
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""


def parse_args():
    parser = argparse.ArgumentParser(
        description="Importa o CSV tratado e materializa a base do mapa em Postgres.",
    )
    parser.add_argument(
        "--database-url",
        default=DEFAULT_DATABASE_URL,
        help=f"URL de conexao do Postgres. Padrao: {DEFAULT_DATABASE_URL}",
    )
    parser.add_argument(
        "--source-csv",
        default="",
        help="CSV de origem. Se vazio, usa o CSV ativo do projeto.",
    )
    parser.add_argument(
        "--keep-existing",
        action="store_true",
        help="Mantem as tabelas e apenas faz truncate/reload.",
    )
    parser.add_argument(
        "--skip-immigrant-records",
        action="store_true",
        help="Nao importa a tabela immigrant_records. Recomendado para deploy gratuito, pois o app nao usa essa tabela em runtime.",
    )
    return parser.parse_args()


def resolve_source_csv(explicit_path):
    if explicit_path:
        return Path(explicit_path).resolve()
    return active_csv_path()


def read_csv_headers(csv_path):
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        headers = next(reader, [])
    if not headers:
        raise RuntimeError(f"CSV sem cabecalho: {csv_path}")
    return headers


def create_immigrant_records_table(conn, headers, keep_existing):
    with conn.cursor() as cur:
        if not keep_existing:
            cur.execute("DROP TABLE IF EXISTS immigrant_records")

        columns_sql = sql.SQL(", ").join(
            sql.SQL("{} text").format(sql.Identifier(header))
            for header in headers
        )
        cur.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS immigrant_records ({})").format(columns_sql)
        )
        cur.execute("TRUNCATE TABLE immigrant_records")

        if "record_id" in headers:
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_immigrant_records_record_id "
                "ON immigrant_records (record_id)"
            )

        if "document_year_num" in headers:
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_immigrant_records_document_year_num "
                "ON immigrant_records (document_year_num)"
            )


def drop_immigrant_records_table(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS immigrant_records")


def copy_csv_into_table(conn, csv_path, table_name, headers):
    copy_sql = sql.SQL(
        "COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')"
    ).format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(sql.Identifier(header) for header in headers),
    )

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                while True:
                    chunk = handle.read(1024 * 1024)
                    if not chunk:
                        break
                    copy.write(chunk)


def create_map_tables(conn, keep_existing):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

        if not keep_existing:
            cur.execute("DROP TABLE IF EXISTS map_points")
            cur.execute("DROP TABLE IF EXISTS map_view_stats")
            cur.execute("DROP TABLE IF EXISTS map_build_meta")

        map_point_columns_sql = ", ".join(
            f"{column_name} {column_type}" for column_name, column_type in MAP_POINT_COLUMNS
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS map_points (
              {map_point_columns_sql}
            )
            """
        )
        cur.execute("TRUNCATE TABLE map_points")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_view_stats (
              view_key text PRIMARY KEY,
              mapped_records integer NOT NULL,
              mapped_cities integer NOT NULL,
              year_min integer,
              year_max integer,
              unmapped_records integer NOT NULL,
              filtered_brazil_records integer NOT NULL
            )
            """
        )
        cur.execute("TRUNCATE TABLE map_view_stats")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_build_meta (
              id bigserial PRIMARY KEY,
              source_path text NOT NULL,
              source_mtime_ns bigint NOT NULL,
              built_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        cur.execute("TRUNCATE TABLE map_build_meta")


def insert_map_points(conn):
    point_columns = [column_name for column_name, _column_type in MAP_POINT_COLUMNS]
    copy_sql = sql.SQL("COPY map_points ({}) FROM STDIN").format(
        sql.SQL(", ").join(sql.Identifier(column_name) for column_name in point_columns)
    )

    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            for view_key in MAP_CONFIG["views"]:
                view_data = load_immigration_points(view_key)
                for point in view_data["points"]:
                    copy.write_row(
                        (
                            point.get("point_id"),
                            point.get("view_mode"),
                            point.get("view_key"),
                            point.get("full_name"),
                            point.get("surname"),
                            point.get("surname_norm"),
                            point.get("surname_search"),
                            point.get("full_name_search"),
                            int(point.get("year")) if point.get("year") is not None else None,
                            point.get("city"),
                            point.get("state"),
                            point.get("locality_label"),
                            float(point.get("lat")),
                            float(point.get("lng")),
                            point.get("origin_place_label"),
                            point.get("origin_place"),
                            point.get("origin_country_label"),
                            point.get("origin_country_display"),
                            point.get("country_filter_key"),
                            point.get("country_filter_label"),
                            point.get("ship_name"),
                            point.get("arrival_port"),
                            point.get("destination_display"),
                            point.get("source"),
                            point.get("source_collection"),
                        )
                    )


def insert_view_stats(conn):
    with conn.cursor() as cur:
        for view_key in MAP_CONFIG["views"]:
            view_data = load_immigration_points(view_key)
            cur.execute(
                MAP_VIEW_STATS_INSERT_SQL,
                (
                    view_key,
                    int(view_data.get("mapped_records") or 0),
                    int(view_data.get("mapped_cities") or 0),
                    int(view_data.get("year_min")) if view_data.get("year_min") is not None else None,
                    int(view_data.get("year_max")) if view_data.get("year_max") is not None else None,
                    int(view_data.get("unmapped_records") or 0),
                    int(view_data.get("filtered_brazil_records") or 0),
                ),
            )


def insert_build_meta(conn, csv_path):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO map_build_meta (source_path, source_mtime_ns)
            VALUES (%s, %s)
            """,
            (str(csv_path), int(csv_path.stat().st_mtime_ns)),
        )


def create_indexes(conn, include_immigrant_records=True):
    with conn.cursor() as cur:
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_map_points_view_year "
            "ON map_points (view_key, year_num)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_map_points_view_order "
            "ON map_points (view_key, year_num, city, surname, point_id)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_map_points_view_country_year "
            "ON map_points (view_key, country_filter_key, year_num)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_map_points_view_city_year "
            "ON map_points (view_key, city, year_num)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_map_points_view_city_country_year "
            "ON map_points (view_key, city, country_filter_key, year_num)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_map_points_view_country_label "
            "ON map_points (view_key, country_filter_label)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_map_points_view_country_label_year "
            "ON map_points (view_key, year_num, country_filter_label)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_map_points_surname_search_trgm "
            "ON map_points USING gin (surname_search gin_trgm_ops)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_map_points_full_name_search_trgm "
            "ON map_points USING gin (full_name_search gin_trgm_ops)"
        )
        if include_immigrant_records:
            cur.execute("ANALYZE immigrant_records")
        cur.execute("ANALYZE map_points")
        cur.execute("ANALYZE map_view_stats")


def main():
    args = parse_args()
    if connect is None or sql is None:
        raise SystemExit(
            "psycopg nao esta instalado. Rode `pip install -r requirements.txt` antes de usar o carregador do Postgres."
        )

    csv_path = resolve_source_csv(args.source_csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV nao encontrado: {csv_path}")

    headers = read_csv_headers(csv_path)
    print(f"[1/5] Conectando ao Postgres: {args.database_url}")
    try:
        with connect(args.database_url, autocommit=False) as conn:
            print(f"[2/5] Recriando tabelas com base em {csv_path.name}")
            if args.skip_immigrant_records:
                drop_immigrant_records_table(conn)
            else:
                create_immigrant_records_table(conn, headers, keep_existing=args.keep_existing)
            create_map_tables(conn, keep_existing=args.keep_existing)

            if args.skip_immigrant_records:
                print("[3/5] Pulando immigrant_records para reduzir o tamanho do banco")
            else:
                print("[3/5] Importando immigrant_records via COPY")
                copy_csv_into_table(conn, csv_path, "immigrant_records", headers)

            print("[4/5] Materializando map_points e map_view_stats")
            insert_map_points(conn)
            insert_view_stats(conn)
            insert_build_meta(conn, csv_path)

            print("[5/5] Criando indices e analisando tabelas")
            create_indexes(conn, include_immigrant_records=not args.skip_immigrant_records)
            conn.commit()
    except Exception as exc:
        raise SystemExit(
            "\n".join(
                [
                    f"Falha ao conectar ou carregar o Postgres em: {args.database_url}",
                    f"Detalhe tecnico: {exc}",
                    "",
                    "Verifique:",
                    "1. Se o Docker Desktop ou o servico do Postgres esta rodando",
                    "2. Se o container foi iniciado com `docker compose up -d postgres`",
                    "3. Se a porta 5432 esta livre e publicada",
                    "4. Se usuario, senha e nome do banco batem com a DATABASE_URL",
                ]
            )
        ) from exc

    print("Carga concluida com sucesso.")


if __name__ == "__main__":
    main()
