const mapLoading = document.getElementById("map-loading");
const mapLoadingText = document.getElementById("map-loading-text");
const yearSlider = document.getElementById("year-slider");
const currentYearLabel = document.getElementById("current-year");
const yearMinLabel = document.getElementById("year-min");
const yearMaxLabel = document.getElementById("year-max");
const totalRecordsLabel = document.getElementById("total-records");
const totalCitiesLabel = document.getElementById("total-cities");
const totalPlacesLabel = document.getElementById("total-places-label");
const rankingTitle = document.getElementById("ranking-title");
const panelTitle = document.getElementById("panel-title");
const panelDescription = document.getElementById("panel-description");
const insightRanking = document.getElementById("insight-ranking");
const locationDetailTitle = document.getElementById("location-detail-title");
const locationDetailContent = document.getElementById("location-detail-content");
const surnameSearchInput = document.getElementById("surname-search");
const clearSearchButton = document.getElementById("clear-search");
const searchFeedback = document.getElementById("search-feedback");
const countryFilterList = document.getElementById("country-filter-list");
const clearCountryFiltersButton = document.getElementById("clear-country-filters");
const countryFilterFeedback = document.getElementById("country-filter-feedback");
const dataLoadNote = document.getElementById("data-load-note");
const dataLoadWarning = document.getElementById("data-load-warning");
const loadAllDataButton = document.getElementById("load-all-data");
const loadLessDataButton = document.getElementById("load-less-data");
const dataLoadStats = document.getElementById("data-load-stats");
const viewButtons = Array.from(document.querySelectorAll("[data-map-view]"));
const rankingModeButtons = Array.from(document.querySelectorAll("[data-ranking-mode]"));

const MARKER_BATCH_SIZE = 900;
const VIEW_MARKER_COLORS = {
  south_brazil: "#f4a259",
  southeast_brazil: "#e08947",
  portugal: "#d96c4f",
  italy: "#be5a38",
  germany: "#8c5634",
  united_states: "#4f7899",
  europe_rest: "#c85b3f",
};
const RANKING_MODE_TITLES = {
  places: null,
  surnames: "Top 10 sobrenomes no ano atual",
  flows: "Top 10 fluxos origem → destino",
};


function setViewButtonsDisabled(disabled) {
  viewButtons.forEach((button) => {
    button.disabled = disabled;
  });
}

function showMapLoading(message = "Carregando dados do mapa...") {
  mapLoadingText.textContent = message;
  mapLoading.classList.remove("is-hidden");
  setViewButtonsDisabled(true);
  if (loadAllDataButton) {
    loadAllDataButton.disabled = true;
  }
  if (loadLessDataButton) {
    loadLessDataButton.disabled = true;
  }
}

function hideMapLoading() {
  mapLoading.classList.add("is-hidden");
  setViewButtonsDisabled(false);
  if (loadAllDataButton) {
    loadAllDataButton.disabled = false;
  }
  if (loadLessDataButton) {
    loadLessDataButton.disabled = false;
  }
}

function renderError(message) {
  hideMapLoading();
  const mapElement = document.getElementById("map");
  mapElement.innerHTML = `<div class="map-error">${message}</div>`;
  mapElement.style.display = "grid";
  mapElement.style.placeItems = "center";
  mapElement.style.padding = "24px";
  mapElement.style.background = "#f7efe2";
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function compactValues(values) {
  return values.map((value) => String(value || "").trim()).filter(Boolean);
}

function firstFilled(...values) {
  return compactValues(values)[0] || "";
}

function asciiFold(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function normalizeQueryText(value) {
  return asciiFold(value).toUpperCase().replace(/\s+/g, " ").trim();
}

function safeText(...values) {
  return firstFilled(...values);
}

function combinePlaceWithState(city, state) {
  const cleanCity = safeText(city);
  const cleanState = safeText(state);
  if (cleanCity && cleanState && cleanCity !== cleanState) {
    return `${cleanCity}, ${cleanState}`;
  }
  return cleanCity || cleanState || "";
}

function computeViewCenter(bounds) {
  return [
    (bounds[0][0] + bounds[1][0]) / 2,
    (bounds[0][1] + bounds[1][1]) / 2,
  ];
}

function formatPlaceLine(point) {
  const parts = compactValues([
    point.city,
    point.state && point.state !== point.city ? point.state : "",
  ]);

  return parts.join(" - ");
}

function buildTooltipContext(point) {
  if (point.view_mode === "europe") {
    const destination = firstFilled(point.destination_display);
    if (destination) {
      return `Destino: ${destination}`;
    }
  }

  return firstFilled(point.locality_label, point.city);
}

function formatTooltip(point) {
  const placeLine = formatPlaceLine(point);
  const contextLine = buildTooltipContext(point);
  const displayName = firstFilled(point.full_name, point.surname);

  return `
    <div class="tooltip-stack">
      <strong>${escapeHtml(displayName)}</strong>
      <span>${escapeHtml(placeLine)}</span>
      <span>Ano: ${point.year}</span>
      <span>${escapeHtml(contextLine)}</span>
    </div>
  `;
}

function popupRow(label, value) {
  if (!String(value || "").trim()) {
    return "";
  }

  return `
    <div class="popup-row">
      <span class="popup-label">${escapeHtml(label)}</span>
      <strong>${escapeHtml(value)}</strong>
    </div>
  `;
}

function formatPopup(point) {
  const displayName = firstFilled(point.full_name, point.surname);
  const sections = [popupRow("Ano", point.year)];

  if (point.view_mode === "europe") {
    sections.push(popupRow("Destino", point.destination_display));
    if (point.view_key !== "united_states") {
      sections.push(
        popupRow(
          firstFilled(point.origin_place_label, "Origem europeia"),
          firstFilled(point.locality_label, point.origin_place, point.city)
        )
      );
    }
    sections.push(
      popupRow(
        firstFilled(point.origin_country_label, "Pais/Regiao"),
        firstFilled(point.origin_country_display, point.state)
      )
    );
  } else {
    sections.push(popupRow("Destino", point.destination_display));
    sections.push(
      popupRow(
        firstFilled(point.origin_country_label, "Pais de origem"),
        firstFilled(point.origin_country_display)
      )
    );
    sections.push(
      popupRow(
        firstFilled(point.origin_place_label, "Origem"),
        firstFilled(point.origin_place)
      )
    );
  }

  sections.push(popupRow("Navio", point.ship_name));

  if (point.view_mode !== "europe") {
    sections.push(popupRow("Porto", point.arrival_port));
  }

  sections.push(popupRow("Fonte", firstFilled(point.source_collection, point.source)));

  return `
    <article class="popup-card popup-card-compact">
      <header class="popup-header">
        <p class="popup-eyebrow">${escapeHtml(point.surname || "Registro")}</p>
        <h3>${escapeHtml(displayName || "Sem nome identificado")}</h3>
      </header>
      <div class="popup-grid popup-grid-compact">
        ${sections.filter(Boolean).join("")}
      </div>
    </article>
  `;
}

function formatPopupLoading(point) {
  const displayName = firstFilled(point.full_name, point.surname);
  return `
    <article class="popup-card popup-card-compact">
      <header class="popup-header">
        <p class="popup-eyebrow">${escapeHtml(point.surname || "Registro")}</p>
        <h3>${escapeHtml(displayName || "Sem nome identificado")}</h3>
      </header>
      <div class="popup-grid popup-grid-compact">
        ${popupRow("Ano", point.year)}
        <div class="popup-row">
          <span class="popup-label">Detalhes</span>
          <strong>Carregando...</strong>
        </div>
      </div>
    </article>
  `;
}

function formatPopupError(point) {
  const displayName = firstFilled(point.full_name, point.surname);
  return `
    <article class="popup-card popup-card-compact">
      <header class="popup-header">
        <p class="popup-eyebrow">${escapeHtml(point.surname || "Registro")}</p>
        <h3>${escapeHtml(displayName || "Sem nome identificado")}</h3>
      </header>
      <div class="popup-grid popup-grid-compact">
        ${popupRow("Ano", point.year)}
        <div class="popup-row">
          <span class="popup-label">Detalhes</span>
          <strong>Falha ao carregar.</strong>
        </div>
      </div>
    </article>
  `;
}

function pointMatchesQuery(point, normalizedQuery) {
  if (!normalizedQuery) {
    return true;
  }

  const surname = normalizeQueryText(point?.surname || "");
  const fullName = normalizeQueryText(point?.full_name || "");
  return surname.includes(normalizedQuery) || fullName.includes(normalizedQuery);
}

function pointCountryLabel(point) {
  return safeText(point?.country_filter_label);
}

function pointFlowOrigin(point) {
  return safeText(
    point?.origin_country_display,
    point?.country_filter_label,
    point?.origin_place,
    "Origem nao identificada"
  );
}

function pointFlowDestination(point) {
  return safeText(
    point?.destination_display,
    combinePlaceWithState(point?.city, point?.state),
    point?.city,
    "Destino nao identificado"
  );
}

function buildCityRankingFromPoints(points) {
  const cityMap = new Map();
  points.forEach((point) => {
    const city = safeText(point?.city);
    if (!city) {
      return;
    }
    const existing = cityMap.get(city) || { city, count: 0, state: safeText(point?.state) };
    existing.count += 1;
    if (!existing.state && point?.state) {
      existing.state = point.state;
    }
    cityMap.set(city, existing);
  });

  return Array.from(cityMap.values()).sort((left, right) => {
    if (right.count !== left.count) {
      return right.count - left.count;
    }
    return left.city.localeCompare(right.city, "pt-BR");
  });
}

function buildSurnameRankingFromPoints(points) {
  const ignoredSurnames = new Set(["NAO CONSTA", "NADA CONSTA", "NAO INFORMADO", "IGNORADO"]);
  const counts = new Map();
  points.forEach((point) => {
    const surname = safeText(point?.surname);
    if (!surname) {
      return;
    }
    if (ignoredSurnames.has(normalizeQueryText(surname))) {
      return;
    }
    counts.set(surname, (counts.get(surname) || 0) + 1);
  });

  return Array.from(counts.entries())
    .sort((left, right) => {
      if (right[1] !== left[1]) {
        return right[1] - left[1];
      }
      return left[0].localeCompare(right[0], "pt-BR");
    })
    .slice(0, 10)
    .map(([surname, count]) => ({ surname, count }));
}

function buildFlowRankingFromPoints(points) {
  const counts = new Map();
  points.forEach((point) => {
    const origin = pointFlowOrigin(point);
    const destination = pointFlowDestination(point);
    const key = `${origin}|||${destination}`;
    counts.set(key, (counts.get(key) || 0) + 1);
  });

  return Array.from(counts.entries())
    .sort((left, right) => {
      if (right[1] !== left[1]) {
        return right[1] - left[1];
      }
      return left[0].localeCompare(right[0], "pt-BR");
    })
    .slice(0, 10)
    .map(([key, count]) => {
      const [origin, destination] = key.split("|||");
      return { origin, destination, count };
    });
}

function buildAutomaticStatsFromPoints(points) {
  const yearTotals = new Map();
  const countryTotals = new Map();
  const cityTotals = new Map();
  const countryKeys = new Set();

  points.forEach((point) => {
    const year = Number(point?.year);
    if (Number.isFinite(year)) {
      yearTotals.set(year, (yearTotals.get(year) || 0) + 1);
    }

    const city = safeText(point?.city);
    if (city) {
      cityTotals.set(city, (cityTotals.get(city) || 0) + 1);
    }

    const countryLabel = pointCountryLabel(point);
    if (countryLabel) {
      countryTotals.set(countryLabel, (countryTotals.get(countryLabel) || 0) + 1);
      countryKeys.add(normalizeQueryText(countryLabel));
    }
  });

  const pickTop = (entries, labelAsNumber = false) => {
    if (!entries.length) {
      return null;
    }
    entries.sort((left, right) => {
      if (right[1] !== left[1]) {
        return right[1] - left[1];
      }
      if (labelAsNumber) {
        return left[0] - right[0];
      }
      return String(left[0]).localeCompare(String(right[0]), "pt-BR");
    });
    return { label: entries[0][0], count: entries[0][1] };
  };

  return {
    peak_year: pickTop(Array.from(yearTotals.entries()), true),
    peak_country: pickTop(Array.from(countryTotals.entries())),
    peak_city: pickTop(Array.from(cityTotals.entries())),
    country_count: countryKeys.size,
  };
}

function buildCountryFilterResponse(availableFilters, points, selectedKeys) {
  const counts = new Map();
  points.forEach((point) => {
    const key = safeText(point?.country_filter_key).toLowerCase();
    if (!key) {
      return;
    }
    counts.set(key, (counts.get(key) || 0) + 1);
  });

  return availableFilters
    .map((filterMeta) => {
      const key = safeText(filterMeta.key).toLowerCase();
      const count = counts.get(key) || 0;
      return { ...filterMeta, key, count };
    })
    .filter((item) => item.count || selectedKeys.has(item.key))
    .sort((left, right) => {
      if (right.count !== left.count) {
        return right.count - left.count;
      }
      return left.label.localeCompare(right.label, "pt-BR");
    });
}

function filterDatasetResponse(dataset, yearValue, surnameQuery, selectedCountries) {
  const normalizedQuery = normalizeQueryText(surnameQuery);
  const pointsBeforeCountry = [];
  const pointsBeforeSurname = [];
  const matchedPoints = [];
  const selectedYear = Number(yearValue);

  for (const point of dataset.points || []) {
    const pointYear = Number(point?.year);
    if (Number.isFinite(selectedYear) && pointYear > selectedYear) {
      break;
    }

    const countryKey = safeText(point?.country_filter_key).toLowerCase();
    const matchesSurname = pointMatchesQuery(point, normalizedQuery);
    const matchesCountry = !selectedCountries.size || selectedCountries.has(countryKey);

    if (!matchesCountry) {
      if (matchesSurname) {
        pointsBeforeCountry.push(point);
      }
      continue;
    }

    pointsBeforeSurname.push(point);
    if (!matchesSurname) {
      continue;
    }

    pointsBeforeCountry.push(point);
    matchedPoints.push(point);
  }

  return { matchedPoints, pointsBeforeCountry, pointsBeforeSurname };
}

function buildLocalResponse(dataset, yearValue, surnameQuery, selectedCountries) {
  const { matchedPoints, pointsBeforeCountry } = filterDatasetResponse(
    dataset,
    yearValue,
    surnameQuery,
    selectedCountries
  );

  const selectedYear = Number(yearValue);
  return {
    ...dataset,
    query_year: Number.isFinite(selectedYear) ? selectedYear : dataset.query_year,
    points: matchedPoints,
    city_totals: buildCityRankingFromPoints(matchedPoints),
    surname_totals: buildSurnameRankingFromPoints(matchedPoints),
    top_flows: buildFlowRankingFromPoints(matchedPoints),
    automatic_stats: buildAutomaticStatsFromPoints(matchedPoints),
    country_filters: buildCountryFilterResponse(
      dataset.country_filters || [],
      pointsBeforeCountry,
      selectedCountries
    ),
    matched_records: matchedPoints.length,
    rendered_records: matchedPoints.length,
    matched_places: new Set(matchedPoints.map((point) => safeText(point?.city)).filter(Boolean)).size,
    client_side_filtered: true,
  };
}

function buildLocationDetailFromPoints(city, points) {
  const selectedCity = safeText(city);
  if (!selectedCity) {
    return null;
  }

  const cityPoints = points.filter((point) => safeText(point?.city) === selectedCity);
  if (!cityPoints.length) {
    return null;
  }

  const nationalityTotals = new Map();
  const periodTotals = new Map();
  const nameTotals = new Map();
  const state = safeText(cityPoints[0]?.state);
  const ignoredNames = new Set(["NAO CONSTA", "NADA CONSTA", "NAO INFORMADO", "IGNORADO"]);

  cityPoints.forEach((point) => {
    const countryLabel = pointCountryLabel(point);
    if (countryLabel) {
      nationalityTotals.set(countryLabel, (nationalityTotals.get(countryLabel) || 0) + 1);
    }

    const year = Number(point?.year);
    if (Number.isFinite(year)) {
      const decade = Math.floor(year / 10) * 10;
      periodTotals.set(decade, (periodTotals.get(decade) || 0) + 1);
    }

    const nameLabel = safeText(point?.full_name, point?.surname);
    if (nameLabel && !ignoredNames.has(normalizeQueryText(nameLabel))) {
      nameTotals.set(nameLabel, (nameTotals.get(nameLabel) || 0) + 1);
    }
  });

  const toSortedList = (map, limit) =>
    Array.from(map.entries())
      .sort((left, right) => {
        if (right[1] !== left[1]) {
          return right[1] - left[1];
        }
        return String(left[0]).localeCompare(String(right[0]), "pt-BR");
      })
      .slice(0, limit)
      .map(([label, count]) => ({ label, count }));

  let peakPeriod = null;
  if (periodTotals.size) {
    const [decade, count] = Array.from(periodTotals.entries()).sort((left, right) => {
      if (right[1] !== left[1]) {
        return right[1] - left[1];
      }
      return left[0] - right[0];
    })[0];
    peakPeriod = { label: `${decade}-${decade + 9}`, count };
  }

  return {
    city: selectedCity,
    state,
    place_label: combinePlaceWithState(selectedCity, state),
    total_records: cityPoints.length,
    top_nationalities: toSortedList(nationalityTotals, 5),
    peak_period: peakPeriod,
    top_names: toSortedList(nameTotals, 8),
  };
}

function renderRankingModeButtons(activeMode) {
  rankingModeButtons.forEach((button) => {
    button.classList.toggle("is-active", button.dataset.rankingMode === activeMode);
  });
}

function renderPlacesRanking(ranking) {
  if (!ranking.length) {
    return "<li>Nenhum lugar ativo no recorte atual.</li>";
  }

  return ranking
    .slice(0, 10)
    .map(
      (item) => `
        <li>
          <button class="ranking-button" type="button" data-city="${escapeHtml(item.city)}">
            <strong>${escapeHtml(item.city)}</strong>
            <span>${item.count.toLocaleString("pt-BR")} imigrante(s)</span>
          </button>
        </li>
      `
    )
    .join("");
}

function renderSurnamesRanking(ranking) {
  if (!ranking.length) {
    return "<li>Nenhum sobrenome dominante no recorte atual.</li>";
  }

  return ranking
    .slice(0, 10)
    .map(
      (item) => `
        <li>
          <strong>${escapeHtml(item.surname)}</strong>
          <span>${item.count.toLocaleString("pt-BR")} registro(s)</span>
        </li>
      `
    )
    .join("");
}

function renderFlowsRanking(flows) {
  if (!flows.length) {
    return "<li>Nenhum fluxo dominante no recorte atual.</li>";
  }

  return flows
    .slice(0, 10)
    .map(
      (item) => `
        <li>
          <strong>${escapeHtml(item.origin)} &rarr; ${escapeHtml(item.destination)}</strong>
          <span>${item.count.toLocaleString("pt-BR")} registro(s)</span>
        </li>
      `
    )
    .join("");
}

function renderInsightRanking(response, activeMode, defaultTitle) {
  if (!insightRanking) {
    return;
  }

  let html = "";
  if (activeMode === "surnames") {
    html = renderSurnamesRanking(response?.surname_totals || []);
  } else if (activeMode === "flows") {
    html = renderFlowsRanking(response?.top_flows || []);
  } else {
    html = renderPlacesRanking(response?.city_totals || []);
  }

  insightRanking.innerHTML = html;
  rankingTitle.textContent = RANKING_MODE_TITLES[activeMode] || defaultTitle;
  renderRankingModeButtons(activeMode);
}


function updateDataLoadControls(response) {
  if (!dataLoadNote || !dataLoadWarning || !loadAllDataButton || !loadLessDataButton) {
    return;
  }

  const matchedRecords = Number(response?.matched_records || 0).toLocaleString("pt-BR");
  const renderedRecords = Number(response?.rendered_records || 0).toLocaleString("pt-BR");
  const loadAllActive = Boolean(response?.load_all);
  const previewMode = Boolean(response?.client_side_filtered) && !loadAllActive;

  if (loadAllActive) {
    dataLoadNote.textContent = `Todos os registros do recorte atual foram carregados: ${renderedRecords} ocorrencia(s) no mapa.`;
    dataLoadWarning.textContent = "Aviso: este modo pode deixar a navegacao mais lenta em bases grandes.";
    loadAllDataButton.hidden = true;
    loadLessDataButton.hidden = false;
    return;
  }

  if (previewMode) {
    dataLoadNote.textContent = `A linha do tempo e os filtros operam sobre a amostra carregada no navegador: ${renderedRecords} ponto(s) ativos neste recorte.`;
    dataLoadWarning.textContent = "Use carregar todos os dados apenas quando precisar, porque isso pode deixar o sistema lento.";
    loadAllDataButton.hidden = false;
    loadLessDataButton.hidden = true;
    return;
  }

  if (response?.sampling_applied) {
    dataLoadNote.textContent = `Devido ao volume da base, o mapa exibe ${renderedRecords} de ${matchedRecords} registros visiveis para responder mais rapido.`;
  } else {
    dataLoadNote.textContent = `O recorte atual esta leve e exibe ${renderedRecords} registro(s) sem precisar reduzir os dados.`;
  }

  dataLoadWarning.textContent = "Aviso: carregar todos os dados pode deixar o sistema lento.";
  loadAllDataButton.hidden = false;
  loadLessDataButton.hidden = true;
}

function renderDataLoadStats(response) {
  if (!dataLoadStats) {
    return;
  }

  const stats = response?.automatic_stats || {};
  const peakYear = stats.peak_year;
  const peakCountry = stats.peak_country;
  const peakCity = stats.peak_city;
  const countryCount = Number(stats.country_count || 0);

  dataLoadStats.innerHTML = `
    <article class="data-load-stat">
      <span class="data-load-stat-label">Registros no recorte</span>
      <strong>${Number(response?.matched_records || 0).toLocaleString("pt-BR")}</strong>
      <span>${response?.load_all ? "Todos carregados no mapa" : `${Number(response?.rendered_records || 0).toLocaleString("pt-BR")} exibidos`}</span>
    </article>
    <article class="data-load-stat">
      <span class="data-load-stat-label">Ano de pico</span>
      <strong>${escapeHtml(peakYear?.label ?? "--")}</strong>
      <span>${peakYear ? `${peakYear.count.toLocaleString("pt-BR")} registro(s)` : "Sem dado"}</span>
    </article>
    <article class="data-load-stat">
      <span class="data-load-stat-label">Pais mais presente</span>
      <strong>${escapeHtml(peakCountry?.label ?? "--")}</strong>
      <span>${peakCountry ? `${peakCountry.count.toLocaleString("pt-BR")} registro(s)` : "Sem dado"}</span>
    </article>
    <article class="data-load-stat">
      <span class="data-load-stat-label">Cidade com maior concentracao</span>
      <strong>${escapeHtml(peakCity?.label ?? "--")}</strong>
      <span>${peakCity ? `${peakCity.count.toLocaleString("pt-BR")} registro(s)` : "Sem dado"}</span>
    </article>
    <article class="data-load-stat">
      <span class="data-load-stat-label">Nacionalidades diferentes</span>
      <strong>${countryCount.toLocaleString("pt-BR")}</strong>
      <span>No recorte atual</span>
    </article>
  `;
}

function resetLocationPanel() {
  if (!locationDetailContent) {
    return;
  }

  locationDetailContent.innerHTML =
    '<div class="location-panel-empty">Nenhuma localidade selecionada.</div>';
  if (locationDetailTitle) {
    locationDetailTitle.textContent = "Localidade selecionada";
  }
}

function renderLocationLoading(city) {
  if (!locationDetailContent) {
    return;
  }

  locationDetailContent.innerHTML = `
    <div class="location-panel-empty">
      Carregando resumo da localidade <strong>${escapeHtml(city)}</strong>...
    </div>
  `;
  if (locationDetailTitle) {
    locationDetailTitle.textContent = `Localidade selecionada: ${city}`;
  }
}

function renderLocationDetail(detail) {
  if (!locationDetailContent) {
    return;
  }

  if (!detail) {
    resetLocationPanel();
    return;
  }

  const nationalities = (detail.top_nationalities || [])
    .map(
      (item) => `
        <li>
          <strong>${escapeHtml(item.label)}</strong>
          <span>${item.count.toLocaleString("pt-BR")}</span>
        </li>
      `
    )
    .join("");

  const names = (detail.top_names || [])
    .map(
      (item) => `
        <li>
          <strong>${escapeHtml(item.label)}</strong>
          <span>${item.count.toLocaleString("pt-BR")}</span>
        </li>
      `
    )
    .join("");

  locationDetailContent.innerHTML = `
    <div class="location-kpis">
      <article class="stat-card">
        <span class="stat-card-label">Total de imigrantes</span>
        <strong>${Number(detail.total_records || 0).toLocaleString("pt-BR")}</strong>
        <span>No recorte atual</span>
      </article>
      <article class="stat-card">
        <span class="stat-card-label">Periodo de maior entrada</span>
        <strong>${escapeHtml(detail.peak_period?.label ?? "--")}</strong>
        <span>${detail.peak_period ? `${detail.peak_period.count.toLocaleString("pt-BR")} registro(s)` : "Sem dado"}</span>
      </article>
    </div>
    <details class="location-mini">
      <summary>Principais nacionalidades</summary>
      <ul class="compact-list">${nationalities || "<li><span>Sem dados</span></li>"}</ul>
    </details>
    <details class="location-mini">
      <summary>Nomes mais frequentes</summary>
      <ul class="compact-list">${names || "<li><span>Sem dados</span></li>"}</ul>
    </details>
  `;

  if (locationDetailTitle) {
    locationDetailTitle.textContent = `Localidade selecionada: ${detail.place_label || detail.city || "--"}`;
  }
}

function renderCountryFilters(filters, selectedKeys) {
  if (!filters.length) {
    countryFilterList.innerHTML = '<p class="muted">Nenhum pais reconhecido para filtro.</p>';
    return;
  }

  countryFilterList.innerHTML = filters
    .map(
      (filter) => `
        <label class="country-filter-option ${
          filter.count === 0 && !selectedKeys.has(filter.key) ? "country-filter-option-disabled" : ""
        }">
          <input
            type="checkbox"
            value="${escapeHtml(filter.key)}"
            ${selectedKeys.has(filter.key) ? "checked" : ""}
            ${filter.count === 0 && !selectedKeys.has(filter.key) ? "disabled" : ""}
          />
          <span class="country-filter-name">${escapeHtml(filter.label)}</span>
          <span class="country-filter-count">${filter.count.toLocaleString("pt-BR")}</span>
        </label>
      `
    )
    .join("");
}

async function fetchJson(url, signal) {
  const response = await fetch(url, { signal });
  if (!response.ok) {
    let message = "Falha ao carregar os dados do mapa.";
    try {
      const errorPayload = await response.json();
      message = errorPayload?.error || message;
    } catch {
      message = response.statusText || message;
    }
    throw new Error(message);
  }
  return response.json();
}

async function loadMap() {
  if (!window.L) {
    renderError("Leaflet nao carregou. Verifique a conexao para os assets externos.");
    return;
  }

  try {
    const config = await fetchJson("/api/map-config");
    const defaultViewKey = config.default_view || "south_brazil";
    const defaultViewConfig = config.views?.[defaultViewKey];

    if (!defaultViewConfig) {
      throw new Error("Configuracao do mapa nao possui uma visao padrao valida.");
    }

    const map = L.map("map", {
      preferCanvas: true,
      zoomControl: false,
      center: computeViewCenter(defaultViewConfig.focus_bounds),
      zoom: defaultViewConfig.initial_zoom,
      minZoom: defaultViewConfig.min_zoom,
      maxZoom: defaultViewConfig.max_zoom,
      maxBounds: defaultViewConfig.max_bounds,
      maxBoundsViscosity: 0.9,
    });

    const pointRenderer = L.canvas({ padding: 0.4 });
    const immigrantLayer = L.layerGroup().addTo(map);
    const datasetCache = new Map();
    const markerCache = new Map();
    const pointDetailCache = new Map();
    const locationDetailCache = new Map();

    let activeViewKey = defaultViewKey;
    let activeDatasetResponse = null;
    let loadAllDataMode = false;
    let activeRankingMode = "places";
    let availableCountryFilters = [];
    let activeCountryKeys = new Set();
    let activeResponse = null;
    let prefetchStarted = false;
    let renderFrame = null;
    let renderToken = 0;
    let requestToken = 0;
    let queryTimer = null;
    let activeFetchController = null;
    let activeLocationCity = "";
    let locationRequestToken = 0;

    L.control.zoom({ position: "topright" }).addTo(map);

    L.tileLayer(config.tile_layer.url, {
      attribution: config.tile_layer.attribution,
      maxZoom: 18,
    }).addTo(map);

    function updateModeButtons(viewKey) {
      viewButtons.forEach((button) => {
        button.classList.toggle("is-active", button.dataset.mapView === viewKey);
      });
    }

    function updateViewCopy(viewKey) {
      const viewMeta = config.views[viewKey];
      panelTitle.textContent = viewMeta.panel_title;
      panelDescription.textContent = viewMeta.panel_description;
      totalPlacesLabel.textContent = viewMeta.total_places_label;
      rankingTitle.textContent = RANKING_MODE_TITLES[activeRankingMode] || viewMeta.ranking_title;
      updateModeButtons(viewKey);
    }

    function applyViewBounds(viewKey) {
      const viewMeta = config.views[viewKey];
      map.options.minZoom = viewMeta.min_zoom;
      map.options.maxZoom = viewMeta.max_zoom;
      map.setMaxBounds(viewMeta.max_bounds);
      map.fitBounds(viewMeta.focus_bounds, { padding: [20, 20] });
    }

    function createMarker(point) {
      const marker = L.circleMarker([point.lat, point.lng], {
        renderer: pointRenderer,
        radius: loadAllDataMode ? 3.2 : 4,
        color: "#174c39",
        weight: 1,
        fillColor: VIEW_MARKER_COLORS[point.view_key] || VIEW_MARKER_COLORS[activeViewKey] || "#f4a259",
        fillOpacity: loadAllDataMode ? 0.72 : 0.88,
      });

      marker.on("mouseover", () => {
        if (loadAllDataMode || marker.getTooltip()) {
          return;
        }
        marker.bindTooltip(formatTooltip(point), {
          sticky: true,
          direction: "top",
          offset: [0, -6],
          className: "immigrant-tooltip",
        });
      });

      marker.on("click", () => {
        if (!marker.getPopup()) {
          marker.bindPopup(formatPopupLoading(point), {
            maxWidth: 260,
            className: "immigrant-popup",
          });
        }
        marker.openPopup();
        loadPointDetails(point, marker).catch(() => {
          if (marker.isPopupOpen()) {
            marker.setPopupContent(formatPopupError(point));
          }
        });
        loadLocationDetails(point.city).catch((error) => {
          if (locationDetailContent) {
            locationDetailContent.innerHTML =
              `<div class="location-panel-empty">${escapeHtml(error.message || "Falha ao carregar o resumo da localidade.")}</div>`;
          }
        });
      });

      return marker;
    }

    async function loadPointDetails(point, marker) {
      const cacheKey = `${point.view_key}:${point.point_id}`;
      if (pointDetailCache.has(cacheKey)) {
        marker.setPopupContent(formatPopup(pointDetailCache.get(cacheKey)));
        return;
      }

      marker.setPopupContent(formatPopupLoading(point));
      const params = new URLSearchParams({
        view: point.view_key,
        point_id: point.point_id,
      });
      const detail = await fetchJson(`/api/point-details?${params.toString()}`);
      pointDetailCache.set(cacheKey, detail);
      if (marker.isPopupOpen()) {
        marker.setPopupContent(formatPopup(detail));
      }
    }

    function buildDatasetParams(viewKey) {
      const params = new URLSearchParams({ view: viewKey });
      if (loadAllDataMode) {
        params.set("load_all", "1");
      }
      return params;
    }

    function buildActiveFilterParams(viewKey) {
      const params = new URLSearchParams({ view: viewKey });
      if (!yearSlider.disabled && yearSlider.value) {
        params.set("year_max", yearSlider.value);
      }
      const rawQuery = surnameSearchInput.value.trim();
      if (rawQuery) {
        params.set("surname", rawQuery);
      }
      if (activeCountryKeys.size) {
        params.set("countries", Array.from(activeCountryKeys).sort().join(","));
      }
      if (loadAllDataMode) {
        params.set("load_all", "1");
      }
      return params;
    }

    async function loadLocationDetails(city) {
      if (!city) {
        resetLocationPanel();
        return;
      }

      activeLocationCity = city;
      const currentToken = ++locationRequestToken;
      const params = buildActiveFilterParams(activeViewKey);
      params.set("city", city);
      const cacheKey = params.toString();
      const localDetail = buildLocationDetailFromPoints(city, activeResponse?.points || []);

      if (locationDetailCache.has(cacheKey)) {
        renderLocationDetail(locationDetailCache.get(cacheKey));
        return;
      }

      if (localDetail) {
        renderLocationDetail(localDetail);
      } else {
        renderLocationLoading(city);
      }
      const detail = await fetchJson(`/api/location-details?${params.toString()}`);
      if (currentToken !== locationRequestToken) {
        return;
      }
      locationDetailCache.set(cacheKey, detail);
      renderLocationDetail(detail);
    }

    function clearMarkers() {
      if (renderFrame !== null) {
        window.cancelAnimationFrame(renderFrame);
        renderFrame = null;
      }
      immigrantLayer.clearLayers();
    }

function updateSearchFeedback(response) {
  const rawQuery = surnameSearchInput.value.trim();
  clearSearchButton.disabled = !rawQuery;
  const previewMode = Boolean(response?.client_side_filtered) && !response?.load_all;

  if (!rawQuery) {
    searchFeedback.textContent = previewMode
      ? `Sem filtro aplicado. O mapa usa ${response.rendered_records.toLocaleString("pt-BR")} ponto(s) da amostra carregada nesta visao.`
      : response?.sampling_applied
      ? `Sem filtro aplicado. O mapa exibe ${response.rendered_records.toLocaleString("pt-BR")} de ${response.matched_records.toLocaleString("pt-BR")} ocorrencias.`
      : "Sem filtro aplicado. O mapa mostra todos os sobrenomes.";
    return;
  }

      if (!response?.matched_records) {
        searchFeedback.textContent = `Nenhuma ocorrencia encontrada para "${rawQuery}".`;
        return;
      }

  searchFeedback.textContent = previewMode
    ? `${response.matched_records.toLocaleString("pt-BR")} ocorrencia(s) encontradas na amostra carregada para "${rawQuery}".`
    : response.sampling_applied
    ? `${response.matched_records.toLocaleString("pt-BR")} ocorrencia(s) para "${rawQuery}", com ${response.rendered_records.toLocaleString("pt-BR")} exibidas no mapa.`
    : `${response.matched_records.toLocaleString("pt-BR")} ocorrencia(s) para "${rawQuery}".`;
}

    function updateCountryFeedback() {
      clearCountryFiltersButton.disabled = !activeCountryKeys.size;

      if (!availableCountryFilters.length) {
        countryFilterFeedback.textContent = "Nenhum pais reconhecido para filtro.";
        return;
      }

      if (!activeCountryKeys.size) {
        countryFilterFeedback.textContent = "Todos os paises disponiveis estao ativos.";
        return;
      }

      const selectedLabels = availableCountryFilters
        .filter((filter) => activeCountryKeys.has(filter.key))
        .map((filter) => filter.label);

      countryFilterFeedback.textContent =
        selectedLabels.length <= 3
          ? `Filtro ativo: ${selectedLabels.join(", ")}.`
          : `Filtro ativo em ${selectedLabels.length} pais(es).`;
    }

    function updateUiFromResponse(response) {
      activeResponse = response;
      availableCountryFilters = response.country_filters || availableCountryFilters || [];

      totalRecordsLabel.textContent = Number(response.mapped_records || 0).toLocaleString("pt-BR");
      totalCitiesLabel.textContent = Number(response.mapped_cities || 0).toLocaleString("pt-BR");
      yearMinLabel.textContent = response.year_min ?? "--";
      yearMaxLabel.textContent = response.year_max ?? "--";
      currentYearLabel.textContent = response.query_year ?? "--";

      renderCountryFilters(availableCountryFilters, activeCountryKeys);
      renderInsightRanking(
        response,
        activeRankingMode,
        config.views[activeViewKey].ranking_title
      );
      renderDataLoadStats(response);
      updateSearchFeedback(response);
      updateCountryFeedback();
      updateDataLoadControls(response);

      if (activeLocationCity) {
        renderLocationDetail(buildLocationDetailFromPoints(activeLocationCity, response.points || []));
      }
    }

    function renderPoints(points, token) {
      if (token !== renderToken) {
        return;
      }

      clearMarkers();

      if (!points.length) {
        hideMapLoading();
        return;
      }

      let index = 0;
      const batchSize = loadAllDataMode ? 2500 : MARKER_BATCH_SIZE;
      function addBatch() {
        if (token !== renderToken) {
          return;
        }

        let additions = 0;
        while (index < points.length && additions < batchSize) {
          const point = points[index];
          let marker = markerCache.get(point.point_id);
          if (!marker) {
            try {
              marker = createMarker(point);
              markerCache.set(point.point_id, marker);
            } catch (error) {
              console.error("Falha ao criar marcador", point, error);
              index += 1;
              continue;
            }
          }
          marker.addTo(immigrantLayer);
          index += 1;
          additions += 1;
        }

        if (index < points.length) {
          renderFrame = window.requestAnimationFrame(addBatch);
          return;
        }

        renderFrame = null;
        hideMapLoading();
      }

      addBatch();
    }

    function buildDatasetUrl(viewKey) {
      return `/api/immigration-points?${buildDatasetParams(viewKey).toString()}`;
    }

    function applyCurrentFilters() {
      if (!activeDatasetResponse) {
        return;
      }

      const selectedYear = yearSlider.disabled
        ? activeDatasetResponse.query_year
        : Number(yearSlider.value || activeDatasetResponse.query_year);
      const localResponse = buildLocalResponse(
        activeDatasetResponse,
        selectedYear,
        surnameSearchInput.value.trim(),
        activeCountryKeys
      );

      currentYearLabel.textContent = localResponse.query_year ?? "--";
      updateUiFromResponse(localResponse);
      renderToken += 1;
      renderPoints(localResponse.points || [], renderToken);
    }

    async function fetchDataset(viewKey, { resetYear = false, useCache = true } = {}) {
      const nextToken = ++requestToken;
      if (activeFetchController) {
        activeFetchController.abort();
      }
      const controller = new AbortController();
      activeFetchController = controller;

      showMapLoading(`Carregando ${loadAllDataMode ? "todos os dados de " : ""}${config.views[viewKey].label}...`);

      const url = buildDatasetUrl(viewKey);
      try {
        const response =
          useCache && datasetCache.has(url)
            ? datasetCache.get(url)
            : await fetchJson(url, controller.signal);

        if (!datasetCache.has(url)) {
          datasetCache.set(url, response);
        }

        if (nextToken !== requestToken || controller.signal.aborted) {
          return;
        }

        activeViewKey = viewKey;
        activeDatasetResponse = response;
        availableCountryFilters = response.country_filters || [];
        updateViewCopy(viewKey);

        if (response.year_min !== null && response.year_max !== null) {
          yearSlider.disabled = false;
          yearSlider.min = response.year_min;
          yearSlider.max = response.year_max;
          if (resetYear || !yearSlider.value) {
            yearSlider.value = response.year_max;
          } else {
            const clampedYear = Math.max(
              Number(response.year_min),
              Math.min(Number(response.year_max), Number(yearSlider.value))
            );
            yearSlider.value = clampedYear;
          }
        } else {
          yearSlider.disabled = true;
        }

        activeCountryKeys = new Set(
          Array.from(activeCountryKeys).filter((key) =>
            availableCountryFilters.some((filter) => filter.key === key)
          )
        );

        applyCurrentFilters();
        if (!prefetchStarted && !loadAllDataMode) {
          prefetchStarted = true;
          window.setTimeout(() => {
            const warmViews = viewButtons
              .map((button) => button.dataset.mapView)
              .filter((viewName) => viewName && viewName !== activeViewKey);
            warmViews.forEach((viewName, index) => {
              window.setTimeout(async () => {
                const warmUrl = buildDatasetUrl(viewName);
                if (datasetCache.has(warmUrl)) {
                  return;
                }
                try {
                  const payload = await fetchJson(warmUrl);
                  datasetCache.set(warmUrl, payload);
                } catch {
                  return;
                }
              }, 400 * (index + 1));
            });
          }, 900);
        }
      } catch (error) {
        if (error.name === "AbortError") {
          return;
        }
        throw error;
      } finally {
        if (activeFetchController === controller) {
          activeFetchController = null;
        }
      }
    }

    function scheduleLocalFilter(delay = 120) {
      if (queryTimer !== null) {
        window.clearTimeout(queryTimer);
      }

      queryTimer = window.setTimeout(() => {
        applyCurrentFilters();
      }, delay);
    }

    rankingModeButtons.forEach((button) => {
      button.addEventListener("click", () => {
        const nextMode = button.dataset.rankingMode;
        if (nextMode === activeRankingMode) {
          return;
        }
        activeRankingMode = nextMode;
        renderInsightRanking(
          activeResponse,
          activeRankingMode,
          config.views[activeViewKey].ranking_title
        );
      });
    });

    if (insightRanking) {
      insightRanking.addEventListener("click", (event) => {
        const trigger = event.target.closest("[data-city]");
        if (!trigger) {
          return;
        }
        const city = trigger.dataset.city;
        if (!city) {
          return;
        }
        loadLocationDetails(city).catch((error) => {
          if (locationDetailContent) {
            locationDetailContent.innerHTML =
              `<div class="location-panel-empty">${escapeHtml(error.message || "Falha ao carregar o resumo da localidade.")}</div>`;
          }
        });
      });
    }

    yearSlider.addEventListener("input", (event) => {
      currentYearLabel.textContent = event.target.value;
      scheduleLocalFilter(0);
    });

    surnameSearchInput.addEventListener("input", () => {
      scheduleLocalFilter(180);
    });

    clearSearchButton.addEventListener("click", () => {
      surnameSearchInput.value = "";
      scheduleLocalFilter(0);
      surnameSearchInput.focus();
    });

    countryFilterList.addEventListener("change", (event) => {
      if (!event.target.matches('input[type="checkbox"]')) {
        return;
      }

      const key = event.target.value;
      if (event.target.checked) {
        activeCountryKeys.add(key);
      } else {
        activeCountryKeys.delete(key);
      }

      scheduleLocalFilter(0);
    });

    clearCountryFiltersButton.addEventListener("click", () => {
      activeCountryKeys = new Set();
      scheduleLocalFilter(0);
    });

    if (loadAllDataButton) {
      loadAllDataButton.addEventListener("click", () => {
        loadAllDataMode = true;
        clearMarkers();
        markerCache.clear();
        fetchDataset(activeViewKey, { resetYear: false }).catch((error) => renderError(error.message));
      });
    }

    if (loadLessDataButton) {
      loadLessDataButton.addEventListener("click", () => {
        loadAllDataMode = false;
        clearMarkers();
        markerCache.clear();
        fetchDataset(activeViewKey, { resetYear: false }).catch((error) => renderError(error.message));
      });
    }

    viewButtons.forEach((button) => {
      button.addEventListener("click", () => {
        const nextView = button.dataset.mapView;
        if (nextView === activeViewKey) {
          return;
        }
        activeCountryKeys = new Set();
        availableCountryFilters = [];
        activeResponse = null;
        activeLocationCity = "";
        locationRequestToken += 1;
        locationDetailCache.clear();
        clearMarkers();
        markerCache.clear();
        countryFilterList.innerHTML = '<p class="muted">Carregando paises...</p>';
        if (insightRanking) {
          insightRanking.innerHTML = "<li>Carregando dados...</li>";
        }
        resetLocationPanel();
        applyViewBounds(nextView);
        fetchDataset(nextView, { resetYear: true }).catch((error) => renderError(error.message));
      });
    });

    updateViewCopy(defaultViewKey);
    await fetchDataset(defaultViewKey, { resetYear: true });
  } catch (error) {
    renderError(error.message);
  }
}

loadMap();
