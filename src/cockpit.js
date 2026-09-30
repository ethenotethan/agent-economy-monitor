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
  const drawer = document.querySelector("#provenance-drawer");
  const drawerContent = document.querySelector("#provenance-content");
  const drawerBackdrop = document.querySelector("#drawer-backdrop");
  let currentView = "pulse";
  let loadGeneration = 0;
  let provenanceGeneration = 0;
  let lastProvenanceTrigger = null;
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

  function element(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function confidence(value) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? `${(numeric * 100).toFixed(2)}%` : "unknown";
  }

  function provenanceButton(provenanceId, context = {}) {
    const button = element("button", "provenance-trigger", "Open provenance →");
    button.type = "button";
    button.dataset.provenanceId = provenanceId || "";
    button.dataset.attributionMethod = context.method || "not applicable";
    button.dataset.confidence = context.confidence || "";
    return button;
  }

  function provenanceButtons(provenanceIds, context = {}) {
    const actions = element("div", "provenance-actions");
    for (const provenanceId of provenanceIds || []) actions.append(provenanceButton(provenanceId, context));
    return actions;
  }

  function relationshipButton(entity) {
    const button = element("button", "graph-node edge-node", `${entity.kind}:${entity.id}`);
    button.type = "button";
    button.dataset.relatedKind = entity.kind;
    button.dataset.relatedId = entity.id;
    return button;
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
      const card = document.createElement(currentView === "buyers" ? "button" : "article");
      card.className = currentView === "pulse" ? "metric-card" : "fact-card";
      if (currentView === "buyers") {
        card.type = "button";
        card.classList.add("dossier-link");
        card.dataset.buyerId = item.id;
        card.setAttribute("aria-label", `Open dossier for ${item.label}`);
      }
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

  function section(titleText, countText, content) {
    const wrapper = element("section", "dossier-section");
    const heading = element("div", "section-heading");
    heading.append(element("h2", "", titleText), element("span", "section-count", countText));
    wrapper.append(heading, content);
    return wrapper;
  }

  function activateView(name) {
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
    return model;
  }

  function renderDossier(model) {
    const buyer = model.buyer;
    const classifications = model.classifications || [];
    const timeline = (model.timeline && model.timeline.items) || [];
    const graph = model.graph || { nodes: [], edges: [] };
    viewRoot.dataset.state = "ready";
    viewRoot.setAttribute("aria-busy", "false");
    viewRoot.replaceChildren();

    const dossier = element("div", "dossier");
    const head = element("header", "dossier-head");
    const identity = element("div");
    identity.append(
      element("div", "eyebrow", "Buyer dossier"),
      element("h2", "dossier-title", buyer.label),
      element("div", "dossier-id", `${buyer.id} · ${(buyer.value && buyer.value.chain_scope) || "chain unknown"}`)
    );
    head.append(identity, provenanceButtons(buyer.provenance_ids));
    dossier.append(head);

    const classificationGrid = element("div", "classification-grid");
    for (const claim of classifications) {
      const card = element("article", "classification-card");
      const top = element("div", "classification-top");
      top.append(element("div", "classification-label", claim.label), element("span", "badge", confidence(claim.confidence)));
      const badges = element("div", "badge-row");
      badges.append(element("span", `badge ${claim.status}`, String(claim.status).toUpperCase()));
      if (claim.is_stale) badges.append(element("span", "badge stale", "STALE"));
      const conflicts = (claim.conflicting_evidence_ids || []).length;
      if (conflicts) badges.append(element("span", "badge conflict", `${conflicts} CONFLICT${conflicts === 1 ? "" : "S"}`));
      const meta = element("div", "classification-meta", `${claim.method} · ${claim.evidence_window_start} → ${claim.evidence_window_end}`);
      const evidence = [...(claim.supporting_evidence_ids || []), ...(claim.conflicting_evidence_ids || [])];
      card.append(top, badges, meta, element("div", "evidence-ids", evidence.join(" · ")), provenanceButtons(claim.provenance_ids, { method: claim.method, confidence: claim.confidence }));
      classificationGrid.append(card);
    }
    if (!classifications.length) classificationGrid.append(element("div", "section-note", "No sealed classification claims."));
    dossier.append(section("Classifications", `${classifications.length} claims`, classificationGrid));

    const graphList = element("div", "graph-list");
    for (const edge of graph.edges || []) {
      const value = edge.value || {};
      const source = value.source || { kind: "unknown", id: "unknown" };
      const target = value.target || { kind: "unknown", id: "unknown" };
      const card = element("article", "graph-edge");
      card.dataset.sourceKind = source.kind;
      card.dataset.targetKind = target.kind;
      const route = element("div", "edge-route");
      route.append(
        relationshipButton(source),
        element("span", "edge-arrow", "→"),
        relationshipButton(target)
      );
      card.append(
        route,
        element("div", "edge-predicate", value.predicate || edge.label),
        element("div", "edge-meta", `${value.direction || "directed"} · ${value.attribution_method || edge.label} · ${confidence(value.confidence)}`),
        provenanceButtons(edge.provenance_ids, { method: value.attribution_method || edge.label, confidence: value.confidence })
      );
      graphList.append(card);
    }
    if (!(graph.edges || []).length) graphList.append(element("div", "section-note", "No attributed counterparties."));
    dossier.append(section("Relationship graph", `${(graph.nodes || []).length} counterparties`, graphList));

    const timelineList = element("div", "timeline-list");
    for (const event of timeline) {
      const card = element("article", "timeline-card");
      const copy = element("div");
      copy.append(element("div", "fact-label", event.label), element("div", "timeline-meta", `${event.value && event.value.settled_at ? event.value.settled_at : event.observed_at} · ${event.id}`));
      card.append(copy, provenanceButtons(event.provenance_ids));
      timelineList.append(card);
    }
    if (!timeline.length) timelineList.append(element("div", "section-note", "No finalized activity in this bounded window."));
    dossier.append(section("Activity timeline", `${timeline.length} events`, timelineList));

    viewRoot.append(dossier);
    updated.textContent = `Observed ${new Date(buyer.observed_at).toLocaleString()}`;
  }

  async function loadDossier(buyerId) {
    const generation = ++loadGeneration;
    activateView("buyers");
    setState("loading");
    try {
      const response = await fetch(`/api/v1/buyers/${encodeURIComponent(buyerId)}/dossier`, { headers: { Accept: "application/json" }, credentials: "same-origin" });
      if (!response.ok) throw new Error(`request failed (${response.status})`);
      const payload = await response.json();
      if (generation !== loadGeneration) return;
      renderDossier(payload.data);
    } catch (error) {
      if (generation !== loadGeneration) return;
      setState("failure", error instanceof Error ? error.message : "The buyer dossier could not be loaded.");
    }
  }

  function renderRelationshipGraph(graph) {
    viewRoot.dataset.state = "ready";
    viewRoot.setAttribute("aria-busy", "false");
    viewRoot.replaceChildren();
    const explorer = element("div", "dossier");
    const head = element("header", "dossier-head");
    const identity = element("div");
    identity.append(
      element("div", "eyebrow", "Relationship explorer"),
      element("h2", "graph-title dossier-title", graph.root.label),
      element("div", "dossier-id", `${graph.root.kind}:${graph.root.id}`)
    );
    head.append(identity, provenanceButtons(graph.root.provenance_ids));
    explorer.append(head);
    const graphList = element("div", "graph-list");
    for (const edge of graph.edges || []) {
      const value = edge.value || {};
      const source = value.source || { kind: "unknown", id: "unknown" };
      const target = value.target || { kind: "unknown", id: "unknown" };
      const card = element("article", "graph-edge");
      card.dataset.sourceKind = source.kind;
      card.dataset.targetKind = target.kind;
      const route = element("div", "edge-route");
      route.append(relationshipButton(source), element("span", "edge-arrow", "→"), relationshipButton(target));
      card.append(
        route,
        element("div", "edge-predicate", value.predicate || edge.label),
        element("div", "edge-meta", `${value.direction || "directed"} · ${value.attribution_method || edge.label} · ${confidence(value.confidence)}`),
        provenanceButtons(edge.provenance_ids, { method: value.attribution_method || edge.label, confidence: value.confidence })
      );
      graphList.append(card);
    }
    if (!(graph.edges || []).length) graphList.append(element("div", "section-note", "No additional attributed relationships in this bounded window."));
    explorer.append(section("Connected entities", `${(graph.nodes || []).length} nodes`, graphList));
    viewRoot.append(explorer);
    updated.textContent = `Observed ${new Date(graph.root.observed_at).toLocaleString()}`;
  }

  async function loadRelationship(kind, id) {
    const generation = ++loadGeneration;
    activateView("graph");
    setState("loading");
    try {
      const response = await fetch(`/api/v1/graph/${encodeURIComponent(kind)}/${encodeURIComponent(id)}`, { headers: { Accept: "application/json" }, credentials: "same-origin" });
      if (!response.ok) throw new Error(`request failed (${response.status})`);
      const payload = await response.json();
      if (generation !== loadGeneration) return;
      renderRelationshipGraph(payload.data);
    } catch (error) {
      if (generation !== loadGeneration) return;
      setState("failure", error instanceof Error ? error.message : "The relationship graph could not be loaded.");
    }
  }

  function closeProvenance() {
    provenanceGeneration += 1;
    drawer.setAttribute("aria-hidden", "true");
    drawerBackdrop.hidden = true;
    if (lastProvenanceTrigger && document.contains(lastProvenanceTrigger)) lastProvenanceTrigger.focus();
    lastProvenanceTrigger = null;
  }

  async function openProvenance(trigger) {
    const provenanceId = trigger.dataset.provenanceId;
    if (!provenanceId) return;
    const generation = ++provenanceGeneration;
    lastProvenanceTrigger = trigger;
    drawer.setAttribute("aria-hidden", "false");
    drawerBackdrop.hidden = false;
    drawerContent.replaceChildren(element("div", "section-note", "Loading evidence lineage…"));
    document.querySelector("#drawer-close").focus();
    try {
      const response = await fetch(`/api/v1/provenance/${encodeURIComponent(provenanceId)}`, { headers: { Accept: "application/json" }, credentials: "same-origin" });
      if (!response.ok) throw new Error(`request failed (${response.status})`);
      const payload = await response.json();
      if (generation !== provenanceGeneration || drawer.getAttribute("aria-hidden") === "true") return;
      const record = payload.data;
      const rows = [
        ["Source", record.source_id],
        ["Observed", record.observed_at],
        ["Parser", record.parser_version],
        ["Provider", record.provider || "not recorded"],
        ["Chain / block", [record.chain_scope, record.block_reference].filter(Boolean).join(" / ") || "not applicable"],
        ["Transaction", record.transaction_reference || "not applicable"],
        ["Finality", record.finality || "not recorded"],
        ["Attribution method", trigger.dataset.attributionMethod || "not applicable"],
        ["Confidence", trigger.dataset.confidence ? confidence(trigger.dataset.confidence) : "not applicable"],
        ["Evidence object", record.evidence_id],
        ["Evidence SHA-256", record.evidence_sha256],
      ];
      drawerContent.replaceChildren();
      for (const [label, value] of rows) {
        const row = element("div", "lineage-row");
        row.append(element("div", "lineage-label", label), element("div", "lineage-value", value));
        drawerContent.append(row);
      }
    } catch (error) {
      if (generation !== provenanceGeneration || drawer.getAttribute("aria-hidden") === "true") return;
      drawerContent.replaceChildren(element("div", "section-note", error instanceof Error ? error.message : "Evidence lineage unavailable."));
    }
  }

  async function loadView(name) {
    const generation = ++loadGeneration;
    const model = activateView(name);
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
    loadRouteFromLocation();
    pollTimer = window.setInterval(loadRouteFromLocation, 30000);
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
    source.addEventListener("refresh", loadRouteFromLocation);
    source.addEventListener("error", beginPolling, { once: true });
  }

  function loadRouteFromLocation() {
    const parameters = new URLSearchParams(location.search);
    const buyerId = parameters.get("buyer");
    const kind = parameters.get("kind");
    const id = parameters.get("id");
    if (buyerId) {
      loadDossier(buyerId);
    } else if (parameters.get("view") === "graph" && kind && id) {
      loadRelationship(kind, id);
    } else {
      loadView(parameters.get("view") || "pulse");
    }
  }

  document.addEventListener("click", event => {
    const provenance = event.target.closest("button[data-provenance-id]");
    if (provenance) {
      openProvenance(provenance);
      return;
    }
    const buyer = event.target.closest("button[data-buyer-id]");
    if (buyer) {
      history.pushState({ view: "buyers", buyer: buyer.dataset.buyerId }, "", `/?view=buyers&buyer=${encodeURIComponent(buyer.dataset.buyerId)}`);
      loadDossier(buyer.dataset.buyerId);
      return;
    }
    const related = event.target.closest("button[data-related-kind]");
    if (related) {
      const kind = related.dataset.relatedKind;
      const id = related.dataset.relatedId;
      if (kind === "buyer") {
        history.pushState({ view: "buyers", buyer: id }, "", `/?view=buyers&buyer=${encodeURIComponent(id)}`);
        loadDossier(id);
      } else {
        history.pushState({ view: "graph", kind, id }, "", `/?view=graph&kind=${encodeURIComponent(kind)}&id=${encodeURIComponent(id)}`);
        loadRelationship(kind, id);
      }
      return;
    }
    const link = event.target.closest("a[data-view]");
    if (!link) return;
    event.preventDefault();
    const view = link.dataset.view;
    history.pushState({ view }, "", `/?view=${encodeURIComponent(view)}`);
    loadView(view);
  });
  document.querySelector("#drawer-close").addEventListener("click", closeProvenance);
  drawerBackdrop.addEventListener("click", closeProvenance);
  document.addEventListener("keydown", event => {
    if (event.key === "Escape") closeProvenance();
  });
  window.addEventListener("popstate", loadRouteFromLocation);
  const compact = window.matchMedia("(max-width: 980px)");
  const markLayout = () => { document.documentElement.dataset.layout = compact.matches ? "compact" : "desktop"; };
  compact.addEventListener?.("change", markLayout);
  markLayout();
  loadRouteFromLocation();
  connectLiveUpdates();
})();
