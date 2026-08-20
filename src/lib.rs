use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ProofOfDelivery {
    pub object_path: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShipmentEvent {
    PickedUp,
    InTransit,
    Delivered,
    DeliveryException,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct LogisticsJob {
    pub shipment_id: String,
    pub event: ShipmentEvent,
    pub proof_of_delivery: Option<ProofOfDelivery>,
    pub exception_note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobDecision {
    AckDelivered { shipment_id: String, proof_path: String },
    AckException { shipment_id: String, note: String },
    Retry { shipment_id: String, reason: &'static str },
}

pub fn decide(job: &LogisticsJob) -> JobDecision {
    match (&job.event, &job.proof_of_delivery, &job.exception_note) {
        (ShipmentEvent::Delivered, Some(proof), _) => JobDecision::AckDelivered {
            shipment_id: job.shipment_id.clone(),
            proof_path: proof.object_path.clone(),
        },
        (ShipmentEvent::Delivered, None, _) => JobDecision::Retry {
            shipment_id: job.shipment_id.clone(),
            reason: "delivered shipment has no proof",
        },
        (ShipmentEvent::DeliveryException, _, Some(note)) if !note.trim().is_empty() => {
            JobDecision::AckException {
                shipment_id: job.shipment_id.clone(),
                note: note.clone(),
            }
        }
        (ShipmentEvent::DeliveryException, _, _) => JobDecision::Retry {
            shipment_id: job.shipment_id.clone(),
            reason: "delivery exception has no operator note",
        },
        _ => JobDecision::Retry {
            shipment_id: job.shipment_id.clone(),
            reason: "shipment is not terminal",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivered_job_without_proof_is_left_for_retry() {
        let job = LogisticsJob {
            shipment_id: "ship-1042".into(),
            event: ShipmentEvent::Delivered,
            proof_of_delivery: None,
            exception_note: None,
        };

        assert_eq!(
            decide(&job),
            JobDecision::Retry {
                shipment_id: "ship-1042".into(),
                reason: "delivered shipment has no proof",
            }
        );
    }
}

