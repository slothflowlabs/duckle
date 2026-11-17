//! Microsoft Excel Online: a workbook in OneDrive or SharePoint Online, read
//! through Microsoft Graph as an app registration (client credentials).
//!
//! Graph's workbook API (worksheets, ranges) takes no app-only token, so the
//! workbook is downloaded instead, which an app with the Files.Read.All
//! application permission may do, and read as src.excel reads a file on disk:
//! the same sheets, range, header row and declared columns.

use crate::*;
use std::io::Read as _;

impl DuckdbEngine {
    pub(crate) fn run_excel_online_source(
        &self,
        db: &Path,
        spec: &plan::ExcelOnlineSourceSpec,
    ) -> Result<String, EngineError> {
        self.check_cancelled()?;
        // Redirects come back here rather than being followed, so Graph's, to
        // an address signed already, is followed without the token.
        let agent = crate::tls::http_agent_no_redirects(&Default::default());
        let token = graph_token(&agent, spec)?;
        self.check_cancelled()?;
        let bytes = download_workbook(&agent, spec, &token)?;
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let tmp = std::env::temp_dir().join(format!(
            "duckle-excel-online-{}-{}-{}.xlsx",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            spec.node_id.replace(|c: char| !c.is_ascii_alphanumeric(), "_")
        ));
        std::fs::write(&tmp, &bytes)
            .map_err(|e| EngineError::Query(format!("excel-online: write {}: {}", tmp.display(), e)))?;
        let mut read = spec.read.clone();
        read["path"] = JsonValue::String(tmp.to_string_lossy().replace('\\', "/"));
        let select = plan::build_excel_source(&read, spec.declared.as_deref());
        let result = self.run(
            Some(db),
            &format!("LOAD excel; CREATE OR REPLACE TABLE {} AS {};", plan::quote_ident(&spec.node_id), select),
            false,
        );
        let _ = std::fs::remove_file(&tmp);
        result?;
        Ok(format!("excel-online: read the workbook ({} bytes) into {}", bytes.len(), spec.node_id))
    }
}

/// An access token for Graph as the app itself: the client credentials grant,
/// for the application permissions the app was given (`.default`).
fn graph_token(agent: &ureq::Agent, spec: &plan::ExcelOnlineSourceSpec) -> Result<String, EngineError> {
    let url = format!("{}/{}/oauth2/v2.0/token", spec.login_endpoint, urlencode_simple(&spec.tenant_id));
    let scope = format!("{}/.default", spec.graph_endpoint);
    let form = [
        ("grant_type", "client_credentials"),
        ("client_id", spec.client_id.as_str()),
        ("client_secret", spec.client_secret.as_str()),
        ("scope", scope.as_str()),
    ];
    match agent.post(&url).send_form(&form) {
        Ok(r) if r.status() == 200 => r
            .into_json::<JsonValue>()
            .ok()
            .and_then(|body| body.get("access_token").and_then(JsonValue::as_str).map(str::to_string))
            .ok_or_else(|| EngineError::Query("excel-online: Microsoft's sign-in answered without a token".into())),
        Ok(r) => Err(EngineError::Query(format!(
            "excel-online: Microsoft's sign-in answered HTTP {}",
            r.status()
        ))),
        Err(ureq::Error::Status(code, r)) => {
            let body: JsonValue = r.into_json().unwrap_or_default();
            // The first line of error_description is the AADSTS code and what it
            // means, a wrong secret or an unknown tenant; trace ids follow it.
            let said = body
                .get("error_description")
                .and_then(JsonValue::as_str)
                .and_then(|d| d.lines().next())
                .or_else(|| body.get("error").and_then(JsonValue::as_str))
                .unwrap_or("no reason given");
            Err(EngineError::Query(format!(
                "excel-online: Microsoft's sign-in refused the app (HTTP {code}): {said}"
            )))
        }
        Err(e) => Err(EngineError::Query(format!("excel-online: sign-in: {e}"))),
    }
}

/// The workbook's bytes. Graph answers with a redirect to a short-lived address
/// that is signed already, so the token goes to Graph and no further.
fn download_workbook(
    agent: &ureq::Agent,
    spec: &plan::ExcelOnlineSourceSpec,
    token: &str,
) -> Result<Vec<u8>, EngineError> {
    let mut url = format!("{}/v1.0/shares/{}/driveItem/content", spec.graph_endpoint, graph_share_id(&spec.workbook));
    let mut bearer = Some(format!("Bearer {token}"));
    for _ in 0..5 {
        let mut req = agent.get(&url);
        if let Some(b) = &bearer {
            req = req.set("Authorization", b);
        }
        let response = match req.call() {
            Ok(r) => r,
            Err(ureq::Error::Status(code, r)) => return Err(refusal(code, r, bearer.is_some())),
            Err(e) => return Err(EngineError::Query(format!("excel-online: download: {e}"))),
        };
        if (300..400).contains(&response.status()) {
            url = response.header("Location").map(str::to_string).ok_or_else(|| {
                EngineError::Query(format!("excel-online: HTTP {} named nowhere to go", response.status()))
            })?;
            bearer = None;
            continue;
        }
        let mut bytes = Vec::new();
        response
            .into_reader()
            .read_to_end(&mut bytes)
            .map_err(|e| EngineError::Query(format!("excel-online: download: {e}")))?;
        return Ok(bytes);
    }
    Err(EngineError::Query("excel-online: the download was redirected more than 5 times".into()))
}

/// What a refusal said, and for Graph's, what to check.
fn refusal(code: u16, response: ureq::Response, from_graph: bool) -> EngineError {
    let body: JsonValue = response.into_json().unwrap_or_default();
    let said = body
        .pointer("/error/message")
        .and_then(JsonValue::as_str)
        .unwrap_or("no reason given")
        .trim_end_matches('.');
    let hint = match code {
        401 | 403 if from_graph => {
            " The app needs the Files.Read.All application permission for Microsoft Graph, with an admin's consent."
        }
        404 if from_graph => " Check the workbook link: Share > Copy link in Excel gives one.",
        _ => "",
    };
    let who = if from_graph { "Graph" } else { "the download" };
    EngineError::Query(format!("excel-online: {who} answered HTTP {code}: {said}.{hint}"))
}

/// The id Graph's /shares takes for a link: "u!" and the link in unpadded
/// base64url.
fn graph_share_id(link: &str) -> String {
    use base64::Engine as _;
    format!("u!{}", base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(link))
}

#[cfg(test)]
mod tests {
    /// Microsoft's own example, and a link whose plain base64 has a '/' and a
    /// pad, which Graph's form has neither of. Both worked out apart from this
    /// code.
    #[test]
    fn a_link_is_encoded_the_way_graph_s_shares_takes_it() {
        assert_eq!(
            super::graph_share_id("https://onedrive.live.com/redir?resid=1231244193912!12&authKey=1201919!12921!1"),
            "u!aHR0cHM6Ly9vbmVkcml2ZS5saXZlLmNvbS9yZWRpcj9yZXNpZD0xMjMxMjQ0MTkzOTEyITEyJmF1dGhLZXk9MTIwMTkxOSExMjkyMSEx"
        );
        assert_eq!(
            super::graph_share_id(
                "https://contoso.sharepoint.com/:x:/s/Finance/EbWhcXUvLy9Gt0j5S5cGsd8BY0KfgR8RT2Gb6s7Wd8Uc?e=4Yk6Pq"
            ),
            "u!aHR0cHM6Ly9jb250b3NvLnNoYXJlcG9pbnQuY29tLzp4Oi9zL0ZpbmFuY2UvRWJXaGNYVXZMeTlHdDBqNVM1Y0dzZDhCWTBLZmdSOFJUMkdiNnM3V2Q4VWM_ZT00WWs2UHE"
        );
    }
}
