use base64::Engine;
use mudu::common::id::OID;
use mudu_binding::universal::uni_oid::UniOid;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

type AppResult<T> = Result<T, String>;

fn serialize_oid_as_unioid<S>(oid: &OID, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    UniOid::from(*oid).serialize(serializer)
}

fn deserialize_oid_from_unioid<'de, D>(deserializer: D) -> Result<OID, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(UniOid::deserialize(deserializer)?.to_oid())
}

fn serialize_oid_vec_as_unioid<S>(oids: &[OID], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let uni_oids: Vec<UniOid> = oids.iter().copied().map(UniOid::from).collect();
    uni_oids.serialize(serializer)
}

fn deserialize_oid_vec_from_unioid<'de, D>(deserializer: D) -> Result<Vec<OID>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Vec::<UniOid>::deserialize(deserializer)?
        .into_iter()
        .map(|oid| oid.to_oid())
        .collect())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerTopology {
    pub worker_index: usize,
    #[serde(
        serialize_with = "serialize_oid_as_unioid",
        deserialize_with = "deserialize_oid_from_unioid"
    )]
    pub worker_id: OID,
    #[serde(
        serialize_with = "serialize_oid_vec_as_unioid",
        deserialize_with = "deserialize_oid_vec_from_unioid"
    )]
    pub partitions: Vec<OID>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerTopology {
    pub worker_count: usize,
    pub workers: Vec<WorkerTopology>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionRouteEntry {
    #[serde(
        serialize_with = "serialize_oid_as_unioid",
        deserialize_with = "deserialize_oid_from_unioid"
    )]
    pub partition_id: OID,
    #[serde(
        serialize_with = "serialize_oid_as_unioid",
        deserialize_with = "deserialize_oid_from_unioid"
    )]
    pub worker_id: OID,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionRouteResponse {
    pub routes: Vec<PartitionRouteEntry>,
}

pub async fn fetch_server_topology(http_addr: &str) -> AppResult<ServerTopology> {
    let response = get_http_json(http_addr, "/mudu/server/topology").await?;
    let data = extract_http_api_data(response)?;
    serde_json::from_value(data).map_err(|e| format!("decode server topology failed: {}", e))
}

pub fn is_server_topology_unsupported(err: &str) -> bool {
    err.contains("server topology is not supported")
        || err.contains("\"code\":\"NotImplemented\"")
}

pub async fn install_app_package(http_addr: &str, mpk_binary: Vec<u8>) -> AppResult<()> {
    let payload = json!({
        "mpk_base64": base64::engine::general_purpose::STANDARD.encode(mpk_binary),
    });
    let response = post_http_json(http_addr, "/mudu/app/install", payload).await?;
    let _ = extract_http_api_data(response)?;
    Ok(())
}

pub async fn route_partition(
    http_addr: &str,
    rule_name: &str,
    key: Option<Vec<String>>,
    start: Option<Vec<String>>,
    end: Option<Vec<String>>,
) -> AppResult<PartitionRouteResponse> {
    let payload = json!({
        "rule_name": rule_name,
        "key": key,
        "start": start,
        "end": end,
    });
    let response = post_http_json(http_addr, "/mudu/partition/route", payload).await?;
    let data = extract_http_api_data(response)?;
    serde_json::from_value(data).map_err(|e| format!("decode partition route failed: {}", e))
}

async fn get_http_json(http_addr: &str, path: &str) -> AppResult<Value> {
    let url = format!("http://{}{}", http_addr, path);
    let client = reqwest::Client::builder()
        .no_proxy()
        .build()
        .map_err(|e| format!("build HTTP client failed: {}", e))?;
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("GET {} failed: {}", url, e))?;
    response
        .json::<Value>()
        .await
        .map_err(|e| format!("decode HTTP response from {} failed: {}", url, e))
}

async fn post_http_json(http_addr: &str, path: &str, payload: Value) -> AppResult<Value> {
    let url = format!("http://{}{}", http_addr, path);
    let client = reqwest::Client::builder()
        .no_proxy()
        .build()
        .map_err(|e| format!("build HTTP client failed: {}", e))?;
    let response = client
        .post(&url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| format!("POST {} failed: {}", url, e))?;
    response
        .json::<Value>()
        .await
        .map_err(|e| format!("decode HTTP response from {} failed: {}", url, e))
}

fn extract_http_api_data(response: Value) -> AppResult<Value> {
    let status = response
        .get("status")
        .and_then(Value::as_i64)
        .ok_or_else(|| "HTTP API response missing numeric status".to_string())?;
    if status == 0 {
        return Ok(response.get("data").cloned().unwrap_or(Value::Null));
    }
    let message = response
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("HTTP API request failed");
    let data = response.get("data").cloned().unwrap_or(Value::Null);
    Err(format!("{}: {}", message, data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_http_api_data_returns_data_on_success() {
        let value = extract_http_api_data(json!({
            "status": 0,
            "message": "ok",
            "data": {"worker_count": 2}
        }))
        .unwrap();
        assert_eq!(value, json!({"worker_count": 2}));
    }

    #[test]
    fn extract_http_api_data_returns_message_on_failure() {
        let err = extract_http_api_data(json!({
            "status": 1001,
            "message": "fail",
            "data": {"reason": "bad request"}
        }))
        .unwrap_err();
        assert!(err.contains("fail"));
        assert!(err.contains("bad request"));
    }

    #[test]
    fn worker_topology_round_trips_oid_as_unioid() {
        let worker = WorkerTopology {
            worker_index: 0,
            worker_id: (1u128 << 100) + 7,
            partitions: vec![(1u128 << 99) + 3],
        };

        let value = serde_json::to_value(&worker).unwrap();
        assert_eq!(
            value["worker_id"],
            json!({ "h": 68719476736u64, "l": 7u64 })
        );
        assert_eq!(
            value["partitions"][0],
            json!({ "h": 34359738368u64, "l": 3u64 })
        );

        let decoded: WorkerTopology = serde_json::from_value(value).unwrap();
        assert_eq!(decoded, worker);
    }

    #[tokio::test]
    async fn install_app_package_rejects_http_failure() {
        let err = install_app_package("127.0.0.1:1", vec![1, 2, 3])
            .await
            .unwrap_err();
        assert!(err.contains("failed") || err.contains("error"));
    }

    #[test]
    fn detect_unsupported_topology_api_errors() {
        assert!(is_server_topology_unsupported(
            "{\"code\":\"NotImplemented\",\"msg\":\"server topology is not supported\"}"
        ));
        assert!(is_server_topology_unsupported(
            "fail to get server topology: server topology is not supported"
        ));
        assert!(!is_server_topology_unsupported("connection refused"));
    }
}
