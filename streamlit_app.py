import streamlit as st
import os
from html import escape

import folium
from streamlit_folium import st_folium

try:
    if "DATABASE_URL" not in os.environ and "DATABASE_URL" in st.secrets:
        os.environ["DATABASE_URL"] = st.secrets["DATABASE_URL"]
except Exception:
    pass

from app import (
    MAP_CONFIG,
    database_boot_message,
    postgres_ready,
    query_immigration_points,
    query_location_details,
    query_point_details,
)


VIEW_ORDER = [
    "south_brazil",
    "southeast_brazil",
    "portugal",
    "germany",
    "italy",
    "europe_rest",
    "united_states",
]

STREAMLIT_RENDER_LIMITS = {
    "south_brazil": 1800,
    "southeast_brazil": 1800,
    "portugal": 1600,
    "germany": 1800,
    "italy": 1800,
    "europe_rest": 1800,
    "united_states": 1400,
}


def first_value(*values):
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def format_int(value):
    if value is None:
        return "--"
    return f"{int(value):,}".replace(",", ".")


def limit_points_for_streamlit(view, points):
    limit = STREAMLIT_RENDER_LIMITS.get(view, 1600)
    if len(points) <= limit:
        return points, False

    sampled = []
    step = len(points) / float(limit)
    cursor = 0.0
    seen_ids = set()

    while len(sampled) < limit and int(cursor) < len(points):
        point = points[min(int(cursor), len(points) - 1)]
        point_id = point.get("point_id")
        if point_id not in seen_ids:
            sampled.append(point)
            seen_ids.add(point_id)
        cursor += step

    return sampled, True


def map_center_from_bounds(bounds):
    south_west, north_east = bounds
    return [
        (float(south_west[0]) + float(north_east[0])) / 2.0,
        (float(south_west[1]) + float(north_east[1])) / 2.0,
    ]


def popup_html(point):
    name = escape(first_value(point.get("full_name"), point.get("surname"), "Registro sem nome"))
    rows = []
    rows.append(f"<div style='font-weight:700;font-size:14px;margin-bottom:6px'>{name}</div>")

    year = point.get("year")
    if year:
        rows.append(f"<div><strong>Ano:</strong> {escape(str(year))}</div>")

    origin = first_value(point.get("origin_place"), point.get("origin_country_display"))
    if origin:
        rows.append(f"<div><strong>Origem:</strong> {escape(origin)}</div>")

    destination = first_value(point.get("destination_display"), point.get("city"))
    if destination:
        rows.append(f"<div><strong>Destino:</strong> {escape(destination)}</div>")

    source = first_value(point.get("source_collection"), point.get("source"))
    if source:
        rows.append(f"<div><strong>Fonte:</strong> {escape(source)}</div>")

    return "".join(rows)


def point_tooltip(point):
    name = first_value(point.get("full_name"), point.get("surname"), "Registro")
    year = point.get("year")
    if year:
        return f"{name} ({year})"
    return name


def build_map(view, points):
    view_meta = MAP_CONFIG["views"][view]
    focus_bounds = view_meta.get("focus_bounds") or view_meta.get("max_bounds")
    center = map_center_from_bounds(focus_bounds)
    stream_map = folium.Map(
        location=center,
        zoom_start=int(view_meta.get("initial_zoom", 5)),
        tiles=None,
        control_scale=True,
        prefer_canvas=True,
        zoom_control=True,
    )
    folium.TileLayer(
        MAP_CONFIG["tile_layer"]["url"],
        attr=MAP_CONFIG["tile_layer"]["attribution"],
    ).add_to(stream_map)
    stream_map.fit_bounds(focus_bounds)

    marker_color = "#d78f2f" if view_meta.get("point_view_mode") == "brazil" else "#2f7058"

    for point in points:
        folium.CircleMarker(
            location=[float(point["lat"]), float(point["lng"])],
            radius=4,
            color="#24453d",
            weight=1,
            fill=True,
            fill_color=marker_color,
            fill_opacity=0.82,
            tooltip=point_tooltip(point),
            popup=folium.Popup(popup_html(point), max_width=280),
        ).add_to(stream_map)

    return stream_map


def nearest_clicked_point(clicked_object, points):
    if not clicked_object:
        return None

    lat = clicked_object.get("lat")
    lng = clicked_object.get("lng")
    if lat is None or lng is None:
        return None

    closest_point = None
    closest_distance = None
    for point in points:
        distance = (float(point["lat"]) - float(lat)) ** 2 + (float(point["lng"]) - float(lng)) ** 2
        if closest_distance is None or distance < closest_distance:
            closest_distance = distance
            closest_point = point
    return closest_point


def render_ranking_list(items, label_key, count_key="count", empty_message="Sem dados para o recorte atual."):
    if not items:
        st.info(empty_message)
        return

    for index, item in enumerate(items[:10], start=1):
        st.markdown(
            f"{index}. **{item[label_key]}** - {format_int(item[count_key])}",
        )


def render_flow_list(items):
    if not items:
        st.info("Sem fluxos para o recorte atual.")
        return

    for index, item in enumerate(items[:10], start=1):
        st.markdown(
            f"{index}. **{item['origin']} -> {item['destination']}** - {format_int(item['count'])}",
        )


st.set_page_config(
    page_title="Cartografia da Imigracao Historica Brasileira",
    page_icon="🗺️",
    layout="wide",
)

st.title("Cartografia da Imigracao Historica Brasileira")
st.caption(
    "Mapa interativo em Streamlit para explorar rotas, origens, destinos e concentracoes da imigracao historica brasileira."
)

if not postgres_ready():
    st.error(database_boot_message())
    if os.getenv("DATABASE_URL"):
        st.info(
            "A `DATABASE_URL` foi detectada neste processo, mas a conexao ainda falhou. "
            "Confirme a URL, o usuario, a senha e se as tabelas foram carregadas no banco remoto."
        )
    else:
        st.info(
            "A `DATABASE_URL` nao foi encontrada no processo do Streamlit. "
            "Defina a variavel de ambiente antes de rodar `streamlit run` ou crie `.streamlit/secrets.toml`."
        )
    st.code(
        "$env:DATABASE_URL=\"postgresql://usuario:senha@host:porta/database\"\n"
        "streamlit run .\\streamlit_app.py",
        language="powershell",
    )
    st.stop()

available_views = [view for view in VIEW_ORDER if view in MAP_CONFIG["views"]]
view_labels = {MAP_CONFIG["views"][view]["label"]: view for view in available_views}

default_view = MAP_CONFIG["default_view"] if MAP_CONFIG["default_view"] in available_views else available_views[0]
default_label = MAP_CONFIG["views"][default_view]["label"]

selected_view_label = st.radio(
    "Modo do mapa",
    options=list(view_labels.keys()),
    index=list(view_labels.keys()).index(default_label),
    horizontal=True,
)
selected_view = view_labels[selected_view_label]
selected_view_meta = MAP_CONFIG["views"][selected_view]

if st.session_state.get("selected_view") != selected_view:
    st.session_state["selected_view"] = selected_view
    st.session_state["selected_point_id"] = None

base_payload = query_immigration_points(selected_view)

with st.sidebar:
    st.subheader(selected_view_meta["panel_title"])
    st.caption(selected_view_meta["panel_description"])

    if base_payload["year_min"] is None or base_payload["year_max"] is None:
        st.warning("Nao ha dados disponiveis para este recorte.")
        st.stop()

    selected_year = st.slider(
        "Linha do tempo",
        min_value=int(base_payload["year_min"]),
        max_value=int(base_payload["year_max"]),
        value=int(base_payload["year_max"]),
        step=1,
    )

    surname_query = st.text_input(
        "Filtrar por sobrenome",
        placeholder="Ex.: Antoni, Schmidt, Monod",
    ).strip()

    country_label_to_key = {
        item["label"]: item["key"]
        for item in base_payload.get("country_filters", [])
        if item.get("label") and item.get("key")
    }
    selected_country_labels = st.multiselect(
        "Filtrar por pais",
        options=list(country_label_to_key.keys()),
    )
    selected_country_keys = tuple(
        country_label_to_key[label] for label in selected_country_labels if label in country_label_to_key
    )

    load_all = st.toggle(
        "Carregar mais pontos",
        value=False,
        help="Aumenta a quantidade de pontos retornados pelo banco. Pode deixar a interface mais lenta.",
    )

payload = query_immigration_points(
    selected_view,
    year_max=selected_year,
    surname_query=surname_query,
    country_keys=selected_country_keys,
    load_all=load_all,
)

render_points, streamlit_sampling = limit_points_for_streamlit(selected_view, payload["points"])

metrics = st.columns(4)
metrics[0].metric("Ano visivel", payload["query_year"] or "--")
metrics[1].metric("Registros no recorte", format_int(payload["matched_records"]))
metrics[2].metric(selected_view_meta.get("total_places_label", "Localidades"), format_int(payload["matched_places"]))
metrics[3].metric("Pontos desenhados", format_int(len(render_points)))

left_col, right_col = st.columns([3.4, 1.5], gap="large")

with left_col:
    stream_map = build_map(selected_view, render_points)
    map_state = st_folium(
        stream_map,
        width=None,
        height=760,
        key=f"folium-map-{selected_view}",
        returned_objects=["last_object_clicked"],
    )

    clicked_point = nearest_clicked_point(map_state.get("last_object_clicked"), render_points)
    if clicked_point:
        st.session_state["selected_point_id"] = clicked_point.get("point_id")

    if payload.get("sampling_applied") or streamlit_sampling:
        st.info(
            "Para manter a navegacao responsiva, o mapa exibe uma amostra controlada dos pontos do recorte atual."
        )

    with st.expander("Estatisticas do recorte", expanded=True):
        automatic_stats = payload.get("automatic_stats") or {}
        stat_cols = st.columns(4)
        peak_year = automatic_stats.get("peak_year") or {}
        peak_country = automatic_stats.get("peak_country") or {}
        peak_city = automatic_stats.get("peak_city") or {}
        stat_cols[0].metric("Ano de pico", first_value(peak_year.get("label"), "--"))
        stat_cols[1].metric("Pais mais presente", first_value(peak_country.get("label"), "--"))
        stat_cols[2].metric("Cidade com maior concentracao", first_value(peak_city.get("label"), "--"))
        stat_cols[3].metric("Nacionalidades", format_int(automatic_stats.get("country_count") or 0))

with right_col:
    st.subheader("Localidade selecionada")
    st.caption("Clique em um ponto no mapa para abrir o resumo do registro e da cidade.")

    selected_point_id = st.session_state.get("selected_point_id")
    selected_point = query_point_details(selected_view, selected_point_id) if selected_point_id else None

    if selected_point:
        point_name = first_value(selected_point.get("full_name"), selected_point.get("surname"), "Registro sem nome")
        st.markdown(f"### {point_name}")

        detail_rows = [
            ("Ano", selected_point.get("year")),
            ("Cidade", first_value(selected_point.get("city"))),
            ("Origem", first_value(selected_point.get("origin_place"), selected_point.get("origin_country_display"))),
            ("Destino", first_value(selected_point.get("destination_display"))),
            ("Navio", first_value(selected_point.get("ship_name"))),
            ("Porto", first_value(selected_point.get("arrival_port"))),
            ("Fonte", first_value(selected_point.get("source_collection"), selected_point.get("source"))),
        ]

        for label, value in detail_rows:
            if value in (None, ""):
                continue
            st.markdown(f"**{label}:** {value}")

        locality_detail = query_location_details(
            selected_view,
            selected_point.get("city"),
            year_max=selected_year,
            surname_query=surname_query,
            country_keys=selected_country_keys,
        )

        if locality_detail:
            with st.expander("Resumo da localidade", expanded=True):
                st.markdown(
                    f"**{locality_detail['place_label']}** - {format_int(locality_detail['total_records'])} registro(s)"
                )

                if locality_detail.get("peak_period"):
                    st.markdown(
                        f"**Periodo de maior entrada:** {locality_detail['peak_period']['label']} "
                        f"({format_int(locality_detail['peak_period']['count'])})"
                    )

                st.markdown("**Principais nacionalidades**")
                render_ranking_list(
                    locality_detail.get("top_nationalities", []),
                    label_key="label",
                    empty_message="Sem nacionalidades para esta localidade.",
                )

                st.markdown("**Nomes mais frequentes**")
                render_ranking_list(
                    locality_detail.get("top_names", []),
                    label_key="label",
                    empty_message="Sem nomes frequentes para esta localidade.",
                )
    else:
        st.info("Nenhum ponto selecionado.")

    hotspots_tab, surnames_tab, flows_tab = st.tabs(["Hotspots", "Sobrenomes", "Fluxos"])

    with hotspots_tab:
        render_ranking_list(payload.get("city_totals", []), label_key="city", empty_message="Sem hotspots neste recorte.")

    with surnames_tab:
        render_ranking_list(
            payload.get("surname_totals", []),
            label_key="surname",
            empty_message="Sem sobrenomes para o recorte atual.",
        )

    with flows_tab:
        render_flow_list(payload.get("top_flows", []))
