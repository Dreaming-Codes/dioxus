//! SSR and server functions on a wasm32 JS host, run under node by `wasm-bindgen-test-runner`.
//!
//! A compile check cannot catch executor bugs: spawning onto a thread pool or a tokio `LocalSet`
//! compiles for wasm32 and only panics once a request is served.
#![cfg(all(target_arch = "wasm32", feature = "server"))]

use dioxus::prelude::*;
use dioxus_server::{
    DioxusRouterExt, IndexHtml, ServeConfig, ServerFunction,
    axum::{Router, body::Body},
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use tower::ServiceExt;
use wasm_bindgen_test::wasm_bindgen_test;

const INDEX: &str = r#"<html><head></head><body><div id="main"></div></body></html>"#;

#[post("/api/echo")]
async fn echo(msg: String) -> Result<String> {
    Ok(format!("echo {msg}"))
}

fn app() -> Element {
    let data = use_server_future(|| echo("ssr".into()))?;
    rsx! { p { "{data:?}" } }
}

fn router() -> Router {
    let index = IndexHtml::new(INDEX, "main").unwrap();
    Router::new().serve_api_application(ServeConfig::with_index_html(index), app)
}

async fn send(request: Request<Body>) -> (StatusCode, String) {
    let response = router().oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

#[wasm_bindgen_test]
async fn ssr_resolves_server_futures() {
    let (status, body) = send(Request::get("/").body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("echo ssr"), "{body}");
}

#[wasm_bindgen_test]
async fn server_fn_round_trips() {
    // Fails if the host did not run the `inventory` constructors.
    let echo = ServerFunction::collect()
        .into_iter()
        .find(|f| f.path().starts_with("/api/echo"))
        .expect("echo is registered");

    let request = Request::post(echo.path())
        .header("content-type", "application/json")
        .body(Body::from(r#"{"msg":"x"}"#))
        .unwrap();
    let (status, body) = send(request).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, r#""echo x""#);
}
