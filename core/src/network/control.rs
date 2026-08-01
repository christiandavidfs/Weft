use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum C2S {
    Hello {
        device_id: String,
        device_name: String,
        media_port: u16,
    },
    RequestTransmit {
        device_id: String,
    },
    ReleaseTransmit {
        device_id: String,
    },
    ClockQuery {
        query_sent_us: u64,
    },
    Leave,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum S2C {
    Welcome {
        session_id: String,
        coordinator_id: String,
    },
    Members {
        members: Vec<MemberWire>,
    },
    MemberJoin {
        member: MemberWire,
    },
    MemberLeave {
        device_id: String,
    },
    TransmitGranted {
        device_id: String,
    },
    TransmitRevoked {
        device_id: String,
    },
    TransmitRequested {
        device_id: String,
        device_name: String,
    },
    TransmitDenied {
        device_id: String,
    },
    ClockReply {
        query_sent_us: u64,
        query_received_us: u64,
        reply_sent_us: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberWire {
    pub device_id: String,
    pub device_name: String,
    pub addr: String,
    pub port: u16,
    pub media_port: u16,
}
