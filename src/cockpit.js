(() => {
  "use strict";

  const views = {
    pulse: { title: "Pulse", eyebrow: "Market activity", description: "Finalized payment activity with source-backed protocol, chain, and buyer signals.", endpoint: "/api/v1/pulse", select: data => data.data || [] },
    buyers: { title: "Buyers", eyebrow: "Behavioral intelligence", description: "Chain-scoped handles and reversible clusters. A handle is not a real-world identity.", endpoint: "/api/v1/buyers", select: data => data.items || [] },
    services: { title: "Services", eyebrow: "Verified capabilities", description: "Observed endpoints, offers, accepted rails, and runtime verification state.", endpoint: "/api/v1/services", select: data => data.items || [] },
    graph: { title: "Graph", eyebrow: "Attributed relationships", description: "Buyer, service, protocol, and settlement relationships appear after a bounded entity selection." },
    investigations: { title: "Investigations", eyebrow: "Cited semantic projection", description: "Only approved, read-only projections from the isolated Agentic Commerce Intelligence wiki appear here." },
    system: { title: "System", eyebrow: "Ingestion health", description: "Source coverage, parser versions, observation counts, and replay freshness.", endpoint: "/api/v1/system", select: data => (data.data && data.data.facts) || [] }
  };
  const stateTemplates = Object.fromEntries(
    [...document.querySelectorAll("template[data-state]")].map(node => [node.dataset.state, node.innerHTML])
  );
  const viewRoot = document.querySelector("#view-root");
  const title = document.querySelector("#view-title");
  const eyebrow = document.querySelector("#view-eyebrow");
  const description = document.querySelector("#view-description");
  const updated = document.querySelector("#updated-at");
  const connection = document.querySelector("#connection-label");
  const connectionDot = document.querySelector("#connection-dot");
  let currentView = "pulse";
  let loadGeneration = 0;
  let pollTimer = null;
  let source = null;

  function setState(name, detail) {
    viewRoot.dataset.state = name;
    viewRoot.setAttribute("aria-busy", name === "loading" ? "true" : "false");
    viewRoot.innerHTML = stateTemplates[name] || "";
    const detailNode = viewRoot.querySelector("[data-state-detail]");
    if (detailNode && detail) detailNode.textContent = detail;
  }

  function isStale(items) {
    const timestamps = items.map(item => Date.parse(item.observed_at)).filter(Number.isFinite);
    return timestamps.length > 0 && Date.now() - Math.max(...timestamps) > 5 * 60 * 1000;
  }

  function metricValue(item) {
    const value = item.value || {};
    return value.amount_atomic || value.settlement_count || value.active_buyers || value.observation_count || "—";
  }

  function renderItems(items) {
    if (!items.length) {
      setState("empty");
      updated.textContent = "No observations";
      return;
    }
    viewRoot.dataset.state = isStale(items) ? "stale" : "ready";
    viewRoot.setAttribute("aria-busy", "false");
    viewRoot.replaceChildren();
    if (isStale(items)) {
      const warning = document.createElement("div");
      warning.className = "stale-banner";
      warning.dataset.state = "stale";
      warning.textContent = "STALE · newest observation is more than five minutes old";
      viewRoot.append(warning);
    }
    const list = document.createElement("div");
    list.className = currentView === "pulse" ? "metric-grid" : "fact-list";
    for (const item of items) {
      const card = document.createElement("article");
      card.className = currentView === "pulse" ? "metric-card" : "fact-card";
      if (currentView === "pulse") {
        const kicker = document.createElement("div");
        kicker.className = "card-kicker";
        const kind = document.createElement("span");
        kind.textContent = item.label;
        const chain = document.createElement("span");
        chain.textContent = (item.value && item.value.chain_scope) || item.kind;
        kicker.append(kind, chain);
        const value = document.createElement("div");
        value.className = "metric-value";
        value.textContent = metricValue(item);
        const detail = document.createElement("div");
        detail.className = "metric-detail";
        detail.textContent = `${(item.value && item.value.asset) || "events"} · ${item.provenance_ids.length} source${item.provenance_ids.length === 1 ? "" : "s"}`;
        card.append(kicker, value, detail);
      } else {
        const identity = document.createElement("div");
        const label = document.createElement("div");
        label.className = "fact-label";
        label.textContent = item.label;
        const id = document.createElement("div");
        id.className = "fact-id";
        id.textContent = item.id;
        identity.append(label, id);
        const value = document.createElement("div");
        value.className = "fact-value";
        value.textContent = JSON.stringify(item.value || {});
        const provenance = document.createElement("a");
        provenance.className = "provenance";
        provenance.textContent = "Provenance ↗";
        provenance.href = `/api/v1/provenance/${encodeURIComponent(item.provenance_ids[0] || "")}`;
        card.append(identity, value, provenance);
      }
      list.append(card);
    }
    viewRoot.append(list);
    const newest = items.map(item => Date.parse(item.observed_at)).filter(Number.isFinite).sort((a, b) => b - a)[0];
    updated.textContent = newest ? `Observed ${new Date(newest).toLocaleString()}` : "Observation time unavailable";
  }

  async function loadView(name) {
    const generation = ++loadGeneration;
    const model = views[name] || views.pulse;
    currentView = views[name] ? name : "pulse";
    document.title = `${model.title} · Agent Economy Monitor`;
    title.textContent = model.title;
    eyebrow.textContent = model.eyebrow;
    description.textContent = model.description;
    document.querySelectorAll("[data-view]").forEach(link => {
      if (link.dataset.view === currentView) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    });
    setState("loading");
    if (!model.endpoint) {
      setState("empty", currentView === "graph" ? "Select a buyer or service from its dossier to open a bounded relationship graph." : "No approved projection has been published to this surface yet.");
      return;
    }
    try {
      const response = await fetch(model.endpoint, { headers: { Accept: "application/json" }, credentials: "same-origin" });
      if (!response.ok) throw new Error(`request failed (${response.status})`);
      const payload = await response.json();
      if (generation !== loadGeneration) return;
      renderItems(model.select(payload));
    } catch (error) {
      if (generation !== loadGeneration) return;
      setState("failure", error instanceof Error ? error.message : "The read model could not be loaded.");
      updated.textContent = "Update failed";
    }
  }

  function beginPolling() {
    if (pollTimer) return;
    if (source) { source.close(); source = null; }
    connection.textContent = "Live unavailable · polling";
    connectionDot.classList.remove("live");
    loadView(currentView);
    pollTimer = window.setInterval(() => loadView(currentView), 30000);
  }

  function connectLiveUpdates() {
    if (!("EventSource" in window)) {
      beginPolling();
      return;
    }
    source = new EventSource("/api/v1/stream");
    source.addEventListener("open", () => {
      connection.textContent = "Live updates";
      connectionDot.classList.add("live");
    });
    source.addEventListener("refresh", () => loadView(currentView));
    source.addEventListener("error", beginPolling, { once: true });
  }

  document.addEventListener("click", event => {
    const link = event.target.closest("a[data-view]");
    if (!link) return;
    event.preventDefault();
    const view = link.dataset.view;
    history.pushState({ view }, "", `/?view=${encodeURIComponent(view)}`);
    loadView(view);
  });
  window.addEventListener("popstate", () => loadView(new URLSearchParams(location.search).get("view") || "pulse"));
  const compact = window.matchMedia("(max-width: 980px)");
  const markLayout = () => { document.documentElement.dataset.layout = compact.matches ? "compact" : "desktop"; };
  compact.addEventListener?.("change", markLayout);
  markLayout();
  loadView(new URLSearchParams(location.search).get("view") || "pulse");
  connectLiveUpdates();
})();
