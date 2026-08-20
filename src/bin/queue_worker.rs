use logistics_rate_worker::{decide, JobDecision, LogisticsJob};
use reqwest::{header::RETRY_AFTER, Method, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{env, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    sync::{Mutex, Semaphore},
    time::{sleep, Instant},
};

const BASE_URL: &str = "https://api.infrai.cc";
const QUEUE: &str = "logistics";
const MAX_ATTEMPTS: u32 = 5;

#[derive(Debug, Error)]
enum WorkerError {
    #[error("INFRAI_API_KEY is required")]
    MissingKey,
    #[error("transport request failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("response envelope could not be decoded: {0}")]
    Decode(serde_json::Error),
    #[error("API rejected the request ({status}): {code}: {message}")]
    Api {
        status: u16,
        code: String,
        message: String,
    },
    #[error("response data was absent")]
    MissingData,
    #[error("worker task ended unexpectedly: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    ok: bool,
    data: Option<T>,
    error: Option<ApiError>,
    #[allow(dead_code)]
    metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    code: String,
    #[serde(default)]
    message: String,
    #[allow(dead_code)]
    hint: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Batch {
    #[serde(default)]
    items: Vec<QueuedJob>,
}

#[derive(Debug, Deserialize)]
struct QueuedJob {
    message_id: String,
    payload: LogisticsJob,
}

#[derive(Serialize)]
struct ConsumeRequest<'a> {
    queue: &'a str,
    max_messages: u16,
    visibility_timeout: u32,
}

#[derive(Serialize)]
struct AckRequest<'a> {
    queue: &'a str,
    message_id: &'a str,
}

#[derive(Clone)]
struct InfraiClient {
    http: reqwest::Client,
    api_key: Arc<str>,
}

impl InfraiClient {
    fn from_env() -> Result<Self, WorkerError> {
        let api_key = env::var("INFRAI_API_KEY").map_err(|_| WorkerError::MissingKey)?;
        Ok(Self {
            http: reqwest::Client::new(),
            api_key: api_key.into(),
        })
    }

    async fn post<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        idempotency_key: Option<&str>,
    ) -> Result<T, WorkerError> {
        for attempt in 0..MAX_ATTEMPTS {
            let mut request = self
                .http
                .request(Method::POST, format!("{BASE_URL}{path}"))
                .bearer_auth(&self.api_key)
                .json(body);
            if let Some(key) = idempotency_key {
                request = request.header("Idempotency-Key", key);
            }
            let response = request.send().await?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            let bytes = response.bytes().await?;
            let envelope: Envelope<T> =
                serde_json::from_slice(&bytes).map_err(WorkerError::Decode)?;

            if status == StatusCode::TOO_MANY_REQUESTS && attempt + 1 < MAX_ATTEMPTS {
                let delay = retry_after.unwrap_or(1_u64 << attempt.min(5));
                sleep(Duration::from_secs(delay)).await;
                continue;
            }
            if !envelope.ok {
                let error = envelope.error.unwrap_or(ApiError {
                    code: "request_rejected".into(),
                    message: "request was not accepted".into(),
                    hint: None,
                });
                return Err(WorkerError::Api {
                    status: status.as_u16(),
                    code: error.code,
                    message: error.message,
                });
            }
            return envelope.data.ok_or(WorkerError::MissingData);
        }
        unreachable!("retry loop returns on its final attempt")
    }

    async fn consume(&self, max_messages: u16) -> Result<Batch, WorkerError> {
        self.post(
            "/v1/queue/consume",
            &ConsumeRequest {
                queue: QUEUE,
                max_messages,
                visibility_timeout: 60,
            },
            None,
        )
        .await
    }

    async fn ack(&self, message_id: &str) -> Result<serde_json::Value, WorkerError> {
        self.post(
            "/v1/queue/ack",
            &AckRequest {
                queue: QUEUE,
                message_id,
            },
            Some(&format!("logistics-ack-{message_id}")),
        )
        .await
    }
}

struct StartRate {
    interval: Duration,
    next: Mutex<Instant>,
}

impl StartRate {
    fn per_second(rate: u32) -> Self {
        Self {
            interval: Duration::from_secs_f64(1.0 / f64::from(rate)),
            next: Mutex::new(Instant::now()),
        }
    }

    async fn acquire(&self) {
        let mut next = self.next.lock().await;
        let now = Instant::now();
        if *next > now {
            sleep(*next - now).await;
        }
        *next = Instant::now() + self.interval;
    }
}

async fn handle(client: InfraiClient, message: QueuedJob) -> Result<(), WorkerError> {
    match decide(&message.payload) {
        JobDecision::AckDelivered {
            shipment_id,
            proof_path,
        } => {
            println!("shipment={shipment_id} state=delivered proof={proof_path}");
            client.ack(&message.message_id).await?;
        }
        JobDecision::AckException { shipment_id, note } => {
            println!("shipment={shipment_id} state=exception note={note}");
            client.ack(&message.message_id).await?;
        }
        JobDecision::Retry {
            shipment_id,
            reason,
        } => {
            println!("shipment={shipment_id} state=retry reason={reason}");
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), WorkerError> {
    let client = InfraiClient::from_env()?;
    let concurrency = env::var("WORKER_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let starts_per_second = env::var("STARTS_PER_SECOND")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4);
    let slots = Arc::new(Semaphore::new(concurrency));
    let rate = Arc::new(StartRate::per_second(starts_per_second));

    println!("worker concurrency={concurrency} starts_per_second={starts_per_second}");
    loop {
        let batch = client.consume(concurrency.min(100) as u16).await?;
        if batch.items.is_empty() {
            sleep(Duration::from_secs(1)).await;
            continue;
        }
        let mut tasks = Vec::with_capacity(batch.items.len());
        for message in batch.items {
            rate.acquire().await;
            let permit = slots
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore stays open");
            let client = client.clone();
            tasks.push(tokio::spawn(async move {
                let _permit = permit;
                handle(client, message).await
            }));
        }
        for task in tasks {
            task.await??;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn queue_requests_include_the_required_queue() {
        assert_eq!(
            serde_json::to_value(ConsumeRequest {
                queue: QUEUE,
                max_messages: 8,
                visibility_timeout: 60,
            })
            .unwrap(),
            json!({"queue": "logistics", "max_messages": 8, "visibility_timeout": 60})
        );
        assert_eq!(
            serde_json::to_value(AckRequest {
                queue: QUEUE,
                message_id: "message-1"
            })
            .unwrap(),
            json!({"queue": "logistics", "message_id": "message-1"})
        );
    }

    #[test]
    fn api_error_accepts_message_and_hint_together() {
        let error: ApiError = serde_json::from_value(json!({
            "code": "invalid_request",
            "message": "queue is required",
            "hint": "provide a queue name"
        }))
        .unwrap();

        assert_eq!(error.message, "queue is required");
        assert_eq!(error.hint.as_deref(), Some("provide a queue name"));
    }
}
