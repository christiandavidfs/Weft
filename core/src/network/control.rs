use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum C2S {
    Hello {
        device_id: String,
        device_name: String,
    },
    RequestTransmit {
        device_id: String,
    },
    ReleaseTransmit {
        device_id: String,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberWire {
    pub device_id: String,
    pub device_name: String,
    pub addr: String,
    pub port: u16,
}
