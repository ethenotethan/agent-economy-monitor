"use strict";

const REPO = "https://github.com/ethenotethan/agent-economy-monitor";
const PAGE_ORDER = ["external", "evidence", "canonical", "projection", "delivery", "experience"];
const PAGE_LABELS = {
  external: "External systems",
  evidence: "Deterministic evidence",
  canonical: "Canonical knowledge",
  projection: "Semantic projection",
  delivery: "Runtime delivery",
  experience: "Experience",
};
const PAGE_COLORS = {
  external: "#f2c66d",
  evidence: "#60d9ef",
  canonical: "#5ee4ad",
  projection: "#ac91f4",
  delivery: "#f37f98",
  experience: "#b8f36b",
};
const AUTHORITY_COPY = {
  evidence: {
    icon: "01",
    title: "Deterministic evidence",
    copy: "Immutable observations, raw protocol evidence, finality, and replay stay factual and reproducible.",
  },
  canonical: {
    icon: "02",
    title: "Canonical knowledge",
    copy: "Versioned entities, reversible buyer clusters, classifications, and claims retain provenance.",
  },
  projection: {
    icon: "03",
    title: "Semantic projection",
    copy: "Bounded, cited LLM narratives explain canonical state without gaining authority to rewrite it.",
  },
};

const state = { model: null, revision: null, filter: "all", query: "", selected: null };
const byId = (id) => document.getElementById(id);
const svgElement = (name, attrs = {}) => {
  const node = document.createElementNS("http://www.w3.org/2000/svg", name);
  Object.entries(attrs).forEach(([key, value]) => node.setAttribute(key, String(value)));
  return node;
};
const element = (name, className, text) => {
  const node = document.createElement(name);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
};
const nodePage = (node) => node.page || "external";
const shortHash = (hash) => `${hash.slice(0, 12)}…${hash.slice(-8)}`;
const sourceUrl = (evidence) => `${REPO}/blob/${state.revision}/${evidence.path}#L${evidence.line}`;
const labelForId = (id) => state.model.interplay.nodes.find((node) => node.id === id)?.label || id;

function validateModel(model) {
  const valid = model?.schema_version === "1.0.0"
    && Array.isArray(model?.interplay?.nodes)
    && Array.isArray(model?.interplay?.edges)
    && Array.isArray(model?.interplay?.flows)
    && Array.isArray(model?.interplay?.invariants)
    && Array.isArray(model?.extraction?.passes)
    && Array.isArray(model?.ci?.jobs);
  if (!valid) throw new Error("architecture.json does not satisfy the expected System Map contract");
}

function renderHero(model) {
  const summary = model.extraction.summary;
  const stats = [
    [summary.entities, "mapped entities"],
    [model.interplay.edges.length, "typed relations"],
    [summary.citations, "source citations"],
  ];
  byId("hero-stats").replaceChildren(...stats.map(([value, label]) => {
    const card = element("div", "hero-stat");
    card.append(element("b", "", String(value)), element("span", "", label));
    return card;
  }));
  byId("model-stamp").textContent = `schema ${model.schema_version} · source tree ${shortHash(model.source_tree_sha256)}`;
  byId("integrity-status").textContent = model.interplay.invariants.every((item) => item.status === "holds") ? "VERIFIED" : "ATTENTION";
}

function renderAuthority(model) {
  const cards = Object.entries(AUTHORITY_COPY).map(([page, data], index) => {
    const count = model.interplay.nodes.filter((node) => nodePage(node) === page).length;
    const card = element("article", `authority-card ${page}`);
    card.dataset.index = String(index + 1).padStart(2, "0");
    card.append(element("div", "card-icon", data.icon), element("h3", "", data.title), element("p", "", data.copy));
    card.append(element("div", "card-count", `${count} model nodes · ${PAGE_LABELS[page]}`));
    return card;
  });
  byId("authority-grid").replaceChildren(...cards);
}

function renderFilters(model) {
  const present = new Set(model.interplay.nodes.map(nodePage));
  const pages = ["all", ...PAGE_ORDER.filter((page) => present.has(page))];
  const buttons = pages.map((page) => {
    const button = element("button", "filter-button", page === "all" ? "All layers" : PAGE_LABELS[page]);
    button.type = "button";
    button.dataset.filter = page;
    button.setAttribute("aria-pressed", String(state.filter === page));
    button.addEventListener("click", () => {
      state.filter = page;
      document.querySelectorAll(".filter-button").forEach((item) => item.setAttribute("aria-pressed", String(item.dataset.filter === page)));
      applyMapFilter();
    });
    return button;
  });
  byId("map-filters").replaceChildren(...buttons);
  byId("node-search").addEventListener("input", (event) => {
    state.query = event.target.value.trim().toLowerCase();
    applyMapFilter();
  });
}

function layoutNodes(nodes) {
  const columns = PAGE_ORDER.map((page) => ({ page, nodes: nodes.filter((node) => nodePage(node) === page) })).filter((column) => column.nodes.length);
  const width = 1460;
  const xPadding = 65;
  const columnWidth = (width - xPadding * 2) / columns.length;
  const positions = new Map();
  columns.forEach((column, columnIndex) => {
    const spacing = Math.min(92, 520 / Math.max(1, column.nodes.length));
    const startY = 105 + Math.max(0, (520 - spacing * (column.nodes.length - 1)) / 2);
    column.nodes.forEach((node, rowIndex) => positions.set(node.id, {
      x: xPadding + columnIndex * columnWidth + columnWidth / 2,
      y: startY + rowIndex * spacing,
      page: column.page,
    }));
  });
  return { width, columns, columnWidth, positions };
}

function renderMap(model) {
  const host = byId("map-canvas");
  const nodes = model.interplay.nodes;
  const { width, columns, columnWidth, positions } = layoutNodes(nodes);
  const svg = svgElement("svg", { viewBox: `0 0 ${width} 680`, role: "group", "aria-labelledby": "topology-svg-title topology-svg-desc" });
  svg.append(svgElement("title", { id: "topology-svg-title" }));
  svg.lastChild.textContent = "Agent Economy Monitor architecture topology";
  svg.append(svgElement("desc", { id: "topology-svg-desc" }));
  svg.lastChild.textContent = "A source-backed directed graph from external evidence through deterministic and canonical layers to semantic projection and product experience.";
  const defs = svgElement("defs");
  const marker = svgElement("marker", { id: "arrow", viewBox: "0 0 10 10", refX: 9, refY: 5, markerWidth: 5, markerHeight: 5, orient: "auto-start-reverse" });
  marker.append(svgElement("path", { d: "M 0 0 L 10 5 L 0 10 z", fill: "#668078" }));
  defs.append(marker);
  svg.append(defs);

  columns.forEach((column, index) => {
    const x = 65 + index * columnWidth;
    const label = svgElement("text", { x: x + columnWidth / 2, y: 45, "text-anchor": "middle", class: "map-column-label" });
    label.textContent = PAGE_LABELS[column.page];
    svg.append(label);
    if (index > 0) svg.append(svgElement("line", { x1: x, y1: 64, x2: x, y2: 635, class: "map-column-rule" }));
  });

  model.interplay.edges.forEach((edge, index) => {
    const source = positions.get(edge.source);
    const target = positions.get(edge.target);
    if (!source || !target) return;
    const direction = target.x >= source.x ? 1 : -1;
    const startX = source.x + direction * 83;
    const endX = target.x - direction * 83;
    const curve = Math.max(38, Math.abs(endX - startX) * .42);
    const path = svgElement("path", {
      d: `M ${startX} ${source.y} C ${startX + direction * curve} ${source.y}, ${endX - direction * curve} ${target.y}, ${endX} ${target.y}`,
      class: "map-edge",
      "data-edge-index": index,
      "data-source": edge.source,
      "data-target": edge.target,
    });
    path.append(svgElement("title"));
    path.lastChild.textContent = `${labelForId(edge.source)} ${edge.relation} ${labelForId(edge.target)}`;
    svg.append(path);
  });

  nodes.forEach((node) => {
    const position = positions.get(node.id);
    if (!position) return;
    const group = svgElement("g", {
      class: "map-node",
      transform: `translate(${position.x - 83} ${position.y - 27})`,
      tabindex: 0,
      role: "button",
      "aria-pressed": "false",
      "aria-label": `${node.label}, ${node.kind}, ${PAGE_LABELS[nodePage(node)]}`,
      "data-node-id": node.id,
      "data-page": nodePage(node),
      style: `--node-color:${PAGE_COLORS[nodePage(node)]}`,
    });
    group.append(svgElement("rect", { width: 166, height: 54, rx: 7 }));
    const kind = svgElement("text", { x: 11, y: 16, class: "node-kind" });
    kind.textContent = node.kind.toUpperCase();
    group.append(kind);
    const label = svgElement("text", { x: 11, y: 36, class: "node-label" });
    const words = node.label.split(" ");
    const first = words.length > 3 ? words.slice(0, Math.ceil(words.length / 2)).join(" ") : node.label;
    label.textContent = first;
    if (first !== node.label) {
      const second = svgElement("tspan", { x: 11, dy: 12 });
      second.textContent = words.slice(Math.ceil(words.length / 2)).join(" ");
      label.append(second);
      kind.setAttribute("y", "13");
      label.setAttribute("y", "29");
    }
    group.append(label);
    const select = () => selectNode(node.id);
    group.addEventListener("click", select);
    group.addEventListener("keydown", (event) => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); select(); } });
    svg.append(group);
  });
  host.replaceChildren(svg);

  const directory = nodes.map((node) => {
    const card = element("button", "directory-card");
    card.type = "button";
    card.dataset.nodeId = node.id;
    card.setAttribute("aria-pressed", "false");
    card.addEventListener("click", () => selectNode(node.id));
    card.append(element("b", "", node.label), element("span", "", `${node.kind} · ${PAGE_LABELS[nodePage(node)]}`));
    return card;
  });
  byId("node-directory").replaceChildren(...directory);
  selectNode(nodes.find((node) => node.id === "collector")?.id || nodes[0]?.id);
  applyMapFilter();
}

function applyMapFilter() {
  const matching = new Set();
  state.model.interplay.nodes.forEach((node) => {
    const pageMatch = state.filter === "all" || nodePage(node) === state.filter;
    const queryMatch = !state.query || `${node.label} ${node.kind} ${node.id}`.toLowerCase().includes(state.query);
    if (pageMatch && queryMatch) matching.add(node.id);
  });
  document.querySelectorAll(".map-node").forEach((node) => node.classList.toggle("is-dimmed", !matching.has(node.dataset.nodeId)));
  document.querySelectorAll(".map-node").forEach((node) => {
    const visible = matching.has(node.dataset.nodeId);
    node.setAttribute("aria-hidden", String(!visible));
    node.setAttribute("tabindex", visible ? "0" : "-1");
    node.style.pointerEvents = visible ? "" : "none";
  });
  document.querySelectorAll(".map-edge").forEach((edge) => {
    edge.classList.toggle("is-dimmed", !matching.has(edge.dataset.source) || !matching.has(edge.dataset.target));
  });
  document.querySelectorAll(".directory-card").forEach((card) => {
    card.hidden = !matching.has(card.dataset.nodeId);
  });
  byId("map-result-count").textContent = `${matching.size} architecture components match the active filter`;
  if (!matching.has(state.selected)) {
    const next = state.model.interplay.nodes.find((node) => matching.has(node.id));
    if (next) selectNode(next.id);
    else {
      document.querySelectorAll(".map-node").forEach((node) => {
        node.classList.remove("selected");
        node.setAttribute("aria-pressed", "false");
      });
      document.querySelectorAll(".directory-card").forEach((card) => card.setAttribute("aria-pressed", "false"));
      document.querySelectorAll(".map-edge").forEach((edge) => edge.classList.remove("active"));
      state.selected = null;
      byId("node-detail").replaceChildren(element("p", "detail-empty", "No components match the active filter."));
    }
  }
}

function selectNode(id) {
  const node = state.model.interplay.nodes.find((candidate) => candidate.id === id);
  if (!node) return;
  state.selected = id;
  document.querySelectorAll(".map-node").forEach((candidate) => {
    const selected = candidate.dataset.nodeId === id;
    candidate.classList.toggle("selected", selected);
    candidate.setAttribute("aria-pressed", String(selected));
  });
  document.querySelectorAll(".directory-card").forEach((candidate) => candidate.setAttribute("aria-pressed", String(candidate.dataset.nodeId === id)));
  document.querySelectorAll(".map-edge").forEach((edge) => edge.classList.toggle("active", edge.dataset.source === id || edge.dataset.target === id));
  const panel = byId("node-detail");
  panel.style.setProperty("--node-color", PAGE_COLORS[nodePage(node)]);
  const kind = element("div", "detail-kind", `${node.kind} / ${PAGE_LABELS[nodePage(node)]}`);
  const title = element("h3", "", node.label);
  const identifier = element("div", "detail-id", node.id);
  const meta = element("div", "detail-meta");
  [["Component", node.component || "boundary"], ["Authority", node.evidence?.[0]?.path === "src/main.rs" && node.kind === "endpoint" ? "mechanical" : "specified"]].forEach(([label, value]) => {
    const item = element("div");
    item.append(element("span", "", label), element("b", "", value));
    meta.append(item);
  });
  const evidenceTitle = element("div", "panel-label", "SOURCE EVIDENCE");
  const evidenceList = element("div", "evidence-list");
  (node.evidence || []).forEach((evidence) => {
    const link = element("a", "evidence-link");
    link.href = sourceUrl(evidence);
    link.append(element("b", "", `${evidence.path}:${evidence.line} ↗`), element("span", "", evidence.excerpt));
    evidenceList.append(link);
  });
  const connections = state.model.interplay.edges.filter((edge) => edge.source === id || edge.target === id);
  const connectionList = element("div", "connection-list");
  connectionList.append(element("h4", "", `${connections.length} TYPED CONNECTION${connections.length === 1 ? "" : "S"}`));
  connections.forEach((edge) => connectionList.append(element("div", "connection-chip", `${labelForId(edge.source)} → ${edge.relation} → ${labelForId(edge.target)}`)));
  panel.replaceChildren(kind, title, identifier, meta, evidenceTitle, evidenceList, connectionList);
}

function renderFlows(model) {
  const cards = model.interplay.flows.map((flow, index) => {
    const card = element("article", "flow-card");
    const header = element("div", "flow-header");
    header.append(element("div", "flow-number", String(index + 1).padStart(2, "0")), element("h3", "", flow.title), element("p", "", flow.summary));
    const track = element("div", "flow-track");
    flow.steps.forEach((step, stepIndex) => {
      const item = element("div", "flow-step");
      item.append(element("b", "", labelForId(step.from)), element("span", "", nodePage(state.model.interplay.nodes.find((node) => node.id === step.from) || {})));
      track.append(item);
      const arrow = element("div", "flow-arrow", step.relation);
      track.append(arrow);
      if (stepIndex === flow.steps.length - 1) {
        const end = element("div", "flow-step");
        end.append(element("b", "", labelForId(step.to)), element("span", "", "destination"));
        track.append(end);
      }
    });
    card.append(header, track);
    return card;
  });
  byId("flow-list").replaceChildren(...cards);
}

function renderStorage(model) {
  const colors = ["#fb923c", "#60d9ef", "#ac91f4", "#5ee4ad"];
  const cards = model.stores.items.map((store, index) => {
    const card = element("article", "storage-card");
    card.style.setProperty("--store-color", colors[index % colors.length]);
    card.append(element("div", "store-kind", store.kind), element("h3", "", store.label));
    const list = element("div", "persistence-list");
    store.persistence.forEach((item) => list.append(element("span", "", item.replaceAll("-", " "))));
    card.append(list);
    return card;
  });
  byId("storage-grid").replaceChildren(...cards);

  const groups = model.interplay.boundary_groups.map((group) => {
    const item = element("article", "boundary-group");
    item.append(element("b", "", group.label), element("p", "", group.description), element("span", "", group.members.map(labelForId).join(" · ")));
    return item;
  });
  byId("boundary-groups").replaceChildren(...groups);
}

function renderInvariants(model) {
  const cards = model.interplay.invariants.map((invariant) => {
    const card = element("article", "invariant-card");
    const body = element("div");
    body.append(element("h3", "", invariant.id.replaceAll("-", " ").toUpperCase()), element("p", "", invariant.why), element("div", "invariant-status", `${invariant.status} · ${invariant.kind}`));
    card.append(element("div", "invariant-check", "✓"), body);
    return card;
  });
  byId("invariant-grid").replaceChildren(...cards);
}

function renderCoverage(model) {
  const passes = model.extraction.passes.map((pass) => {
    const card = element("article", "pass-card");
    const copy = element("div");
    copy.append(element("h3", "", pass.id), element("p", "", pass.description));
    const metrics = element("div", "pass-metrics");
    [[pass.files, "files"], [pass.citations, "citations"]].forEach(([value, label]) => {
      const metric = element("span");
      metric.append(element("b", "", String(value)), document.createTextNode(label));
      metrics.append(metric);
    });
    card.append(copy, metrics);
    return card;
  });
  byId("extraction-passes").replaceChildren(...passes);
  const unresolved = model.extraction.coverage.unresolved.map((item) => {
    const card = element("article", "unresolved-item");
    card.append(element("h3", "", item.id), element("p", "", item.reason));
    return card;
  });
  byId("unresolved-list").replaceChildren(...unresolved);
}

function renderDelivery(model) {
  const nodes = [];
  const gateJobs = model.ci.jobs.filter((job) => job.role === "gate");
  const postMergeJobs = model.ci.jobs.filter((job) => job.role === "post-merge");
  const gateWorkflows = new Set(gateJobs.map((job) => job.workflow));
  const trigger = element("article", "delivery-node");
  trigger.append(element("div", "job-role", "TRIGGER"), element("h3", "", "Pull request"), element("code", "", model.ci.workflows.filter((workflow) => gateWorkflows.has(workflow.id)).map((workflow) => workflow.name).join(" · ")));
  nodes.push(trigger, element("div", "delivery-join", "FANS OUT"));
  gateJobs.forEach((job, index) => {
    const node = element("article", "delivery-node");
    node.append(element("div", "job-role", job.role), element("h3", "", job.name), element("code", "", job.scripts.join(" · ")));
    nodes.push(node);
    if (index < gateJobs.length - 1) nodes.push(element("div", "delivery-join", "+"));
  });
  nodes.push(element("div", "delivery-join", "ALL GREEN"));
  const merge = element("article", "delivery-node");
  merge.append(element("div", "job-role", "MERGE AUTHORITY"), element("h3", "", model.ci.merge.label), element("code", "", `${model.ci.merge.inputs.length} required gate inputs`));
  nodes.push(merge);
  postMergeJobs.forEach((job) => {
    nodes.push(element("div", "delivery-join", job.needs.length ? "THEN" : "PUSH MAIN"));
    const node = element("article", "delivery-node");
    node.append(element("div", "job-role", job.role), element("h3", "", job.name), element("code", "", job.scripts.join(" · ") || "GitHub Pages deployment"));
    nodes.push(node);
  });
  byId("delivery-pipeline").replaceChildren(...nodes);
}

function renderProvenance(model) {
  const summary = model.extraction.summary;
  const stats = [[summary.files, "inventory files"], [model.inventory.lines, "source lines"], [summary.entities_with_origin, "entities with origin"], [summary.passes, "extraction passes"]];
  byId("provenance-stats").replaceChildren(...stats.map(([value, label]) => {
    const item = element("div", "provenance-stat");
    item.append(element("b", "", String(value)), element("span", "", label));
    return item;
  }));
  const seen = new Set();
  const citations = [];
  model.interplay.nodes.forEach((node) => (node.evidence || []).forEach((evidence) => {
    const key = `${evidence.path}:${evidence.line}`;
    if (seen.has(key)) return;
    seen.add(key);
    const link = element("a", "citation");
    link.href = sourceUrl(evidence);
    link.append(element("b", "", `${key} ↗`), element("span", "", evidence.excerpt));
    citations.push(link);
  }));
  byId("citation-list").replaceChildren(...citations);
}

function fail(error) {
  console.error(error);
  byId("model-stamp").textContent = "Architecture model failed to load. Open architecture.json for the raw contract.";
  byId("integrity-status").textContent = "UNAVAILABLE";
  byId("map-canvas").replaceChildren(element("p", "detail-empty", "Unable to render the System Map. The canonical JSON remains linked below."));
}

async function main() {
  try {
    const [response, metadataResponse] = await Promise.all([
      fetch("architecture.json", { cache: "no-store" }),
      fetch("build-meta.json", { cache: "no-store" }),
    ]);
    if (!response.ok) throw new Error(`architecture.json returned ${response.status}`);
    if (!metadataResponse.ok) throw new Error(`build-meta.json returned ${metadataResponse.status}`);
    const [model, metadata] = await Promise.all([response.json(), metadataResponse.json()]);
    validateModel(model);
    if (metadata.source_tree_sha256 !== model.source_tree_sha256 || !/^[0-9a-f]{40,64}$/i.test(metadata.source_revision)) {
      throw new Error("build metadata does not match the canonical architecture model");
    }
    state.model = model;
    state.revision = metadata.source_revision;
    renderHero(model);
    renderAuthority(model);
    renderFilters(model);
    renderMap(model);
    renderFlows(model);
    renderStorage(model);
    renderInvariants(model);
    renderCoverage(model);
    renderDelivery(model);
    renderProvenance(model);
  } catch (error) {
    fail(error);
  }
}

main();
