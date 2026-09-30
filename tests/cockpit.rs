use agent_economy_monitor::cockpit::cockpit_router;
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use tower::ServiceExt;

#[tokio::test]
async fn pulse_is_the_landing_view_with_complete_cockpit_navigation() {
    let response = cockpit_router()
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/html; charset=utf-8"
    );
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();

    assert!(html.contains("<title>Pulse · Agent Economy Monitor</title>"));
    assert!(html.contains("data-view=\"pulse\" aria-current=\"page\""));
    for view in [
        "pulse",
        "buyers",
        "services",
        "graph",
        "investigations",
        "system",
    ] {
        assert!(html.contains(&format!("data-view=\"{view}\"")), "{view}");
    }
    for state in ["loading", "empty", "stale", "failure"] {
        assert!(html.contains(&format!("data-state=\"{state}\"")), "{state}");
    }
}

#[tokio::test]
async fn live_update_stream_is_event_stream_and_emits_refresh_signal() {
    let response = cockpit_router()
        .oneshot(
            Request::builder()
                .uri("/api/v1/stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/event-stream"
    );
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-cache");
    let mut stream = response.into_body().into_data_stream();
    let frame = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
        .await
        .expect("stream must emit immediately")
        .expect("stream must stay open")
        .expect("stream frame must be readable");
    let body = String::from_utf8(frame.to_vec()).unwrap();
    assert!(body.contains("event: refresh"));
}

use futures_util::StreamExt;
