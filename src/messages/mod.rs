use serde::{Deserialize, Serialize};

use self::codec::DResponse;

pub mod codec;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct GetResultReq {
    pub id: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DResult {
    pub data: Option<DResponse>,
    pub error: Option<String>,
    pub code: u16,
}

impl DResult {
    pub fn missed() -> Self {
        Self {
            data: None,
            error: Some("task missed".to_string()),
            code: 604,
        }
    }

    pub fn running() -> Self {
        Self {
            data: None,
            error: Some("task running".to_string()),
            code: 605,
        }
    }

    pub fn completed(data: DResponse) -> Self {
        Self {
            data: Some(data),
            error: None,
            code: 606,
        }
    }
}
