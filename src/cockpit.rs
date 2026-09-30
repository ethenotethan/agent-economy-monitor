use std::{convert::Infallible, time::Duration};

use axum::{
    Router,
    http::header,
    response::{Html, IntoResponse, Sse, sse::Event},
    routing::get,
};
use leptos::prelude::*;
use tokio_stream::{StreamExt, wrappers::IntervalStream};

const STYLE: &str = include_str!("cockpit.css");
const CLIENT: &str = include_str!("cockpit.js");

pub fn cockpit_router() -> Router {
    mount_cockpit(Router::new())
}

pub fn mount_cockpit(router: Router) -> Router {
    router
        .route("/", get(cockpit))
        .route("/api/v1/stream", get(live_updates))
}

pub fn cockpit_document() -> String {
    format!("<!doctype html>{}", view! { <Cockpit/> }.to_html())
}

async fn cockpit() -> Html<String> {
    Html(cockpit_document())
}

async fn live_updates() -> impl IntoResponse {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let stream = IntervalStream::new(interval).map(|_| {
        Ok::<_, Infallible>(
            Event::default()
                .event("refresh")
                .data("read-models-changed"),
        )
    });
    (
        [(header::CACHE_CONTROL, "no-cache")],
        Sse::new(stream).keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        ),
    )
}

#[component]
fn Cockpit() -> impl IntoView {
    view! {
        <html lang="en" data-layout="desktop">
            <head>
                <meta charset="utf-8"/>
                <meta name="viewport" content="width=device-width, initial-scale=1"/>
                <meta name="color-scheme" content="dark"/>
                <title>"Pulse · Agent Economy Monitor"</title>
                <style inner_html=STYLE></style>
            </head>
            <body>
                <div class="shell">
                    <aside class="sidebar" aria-label="Primary navigation">
                        <div class="brand">
                            <div class="brand-mark" aria-hidden="true">"A/M"</div>
                            <div class="brand-copy">
                                <strong>"Agent Economy"</strong>
                                <span>"Intelligence monitor"</span>
                            </div>
                        </div>
                        <nav>
                            <NavLink view="pulse" icon="01" label="Pulse" current=true/>
                            <NavLink view="buyers" icon="02" label="Buyers" current=false/>
                            <NavLink view="services" icon="03" label="Services" current=false/>
                            <NavLink view="graph" icon="04" label="Graph" current=false/>
                            <NavLink view="investigations" icon="05" label="Investigations" current=false/>
                            <NavLink view="system" icon="06" label="System" current=false/>
                        </nav>
                        <div class="sidebar-foot">
                            <div class="connection">
                                <span id="connection-dot" class="dot"></span>
                                <span id="connection-label">"Connecting"</span>
                            </div>
                            <div>"Canonical reads · cited projections"</div>
                        </div>
                    </aside>
                    <main class="content">
                        <header class="topbar">
                            <div class="breadcrumb">"INTELLIGENCE / " <strong id="view-eyebrow">"MARKET ACTIVITY"</strong></div>
                            <div class="live-badge"><span class="dot"></span>" PRIVATE COCKPIT"</div>
                        </header>
                        <section class="hero" aria-labelledby="view-title">
                            <div>
                                <div class="eyebrow" id="view-eyebrow-mobile">"EVIDENCE-BACKED SIGNAL"</div>
                                <h1 id="view-title">"Pulse"</h1>
                                <p class="lede" id="view-description">"Finalized payment activity with source-backed protocol, chain, and buyer signals."</p>
                            </div>
                            <div class="timestamp" id="updated-at" aria-live="polite">"Loading observations"</div>
                        </section>
                        <section id="view-root" class="view" data-state="loading" aria-live="polite" aria-busy="true">
                            <div class="skeletons" aria-label="Loading Pulse">
                                <div class="skeleton"></div>
                                <div class="skeleton"></div>
                                <div class="skeleton"></div>
                            </div>
                        </section>
                    </main>
                </div>
                <button id="drawer-backdrop" class="drawer-backdrop" aria-label="Close provenance" hidden></button>
                <aside id="provenance-drawer" class="provenance-drawer" role="dialog" aria-modal="true" aria-label="Evidence lineage" aria-hidden="true" tabindex="-1">
                    <div class="drawer-head">
                        <div><div class="eyebrow">"Evidence lineage"</div><h2>"Provenance"</h2></div>
                        <button id="drawer-close" class="drawer-close" type="button" aria-label="Close provenance drawer">"Close"</button>
                    </div>
                    <div id="provenance-content" class="drawer-content"></div>
                </aside>
                <template data-state="loading">
                    <div class="skeletons" aria-label="Loading view">
                        <div class="skeleton"></div><div class="skeleton"></div><div class="skeleton"></div>
                    </div>
                </template>
                <template data-state="empty">
                    <div class="state-card" data-state="empty">
                        <div><div class="state-mark">"NO CANONICAL FACTS"</div><h2>"Nothing to show yet"</h2><p data-state-detail>"Collection is healthy, but no verified observations exist for this view."</p></div>
                    </div>
                </template>
                <template data-state="stale">
                    <div class="state-card" data-state="stale">
                        <div><div class="state-mark">"STALE READ MODEL"</div><h2>"Freshness window exceeded"</h2><p data-state-detail>"The latest canonical observation is older than five minutes. Existing facts remain visible with their observation times."</p></div>
                    </div>
                </template>
                <template data-state="failure">
                    <div class="state-card" data-state="failure">
                        <div><div class="state-mark">"READ FAILED"</div><h2>"Intelligence surface unavailable"</h2><p data-state-detail>"The bounded read model could not be loaded. No cached value is presented as current."</p></div>
                    </div>
                </template>
                <script inner_html=CLIENT></script>
            </body>
        </html>
    }
}

#[component]
fn NavLink(
    view: &'static str,
    icon: &'static str,
    label: &'static str,
    current: bool,
) -> impl IntoView {
    let href = format!("/?view={view}");
    view! {
        <a class="nav-link" href=href data-view=view aria-current=current.then_some("page")>
            <span class="nav-icon" aria-hidden="true">{icon}</span>
            <span class="nav-label">{label}</span>
        </a>
    }
}
