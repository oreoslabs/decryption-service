use anyhow::Result;
use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::io::Write;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct RegisterWorker {
    pub name: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SingleRequest {
    pub serialized_note: String,
    pub incoming_view_key: String,
    pub outgoing_view_key: String,
    pub view_key: String,
    pub current_note_index: Option<u64>,
    pub decrypt_for_spender: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DRequest {
    pub id: String,
    pub data: Vec<SingleRequest>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SingleResponse {
    pub index: Option<u64>,
    pub for_spender: bool,
    pub hash: String,
    pub nullifier: Option<String>,
    pub serialized_note: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DResponse {
    pub id: String,
    pub data: Vec<SingleResponse>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum DMessage {
    RegisterWorker(RegisterWorker),
    DRequest(DRequest),
    DResponse(DResponse),
}

#[derive(Default)]
pub struct DMessageCodec {
    cursor: usize,
}

impl Encoder<DMessage> for DMessageCodec {
    type Error = anyhow::Error;
    fn encode(&mut self, message: DMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let json_string = serde_json::to_string(&message).unwrap();
        dst.writer().write_all(json_string.as_bytes())?;
        dst.writer().write_all("\n".as_bytes())?;
        Ok(())
    }
}

impl Decoder for DMessageCodec {
    type Error = anyhow::Error;
    type Item = DMessage;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut i = self.cursor;
        while i < src.len() {
            if src[i] == 10u8 {
                self.cursor = 0;
                let mut data = src.split_to(i + 1);
                unsafe {
                    data.set_len(i);
                }
                src.reserve(100);
                let message = serde_json::from_slice(&data[..])?;
                return Ok(Some(message));
            }
            i += 1;
        }
        self.cursor = i;
        Ok(None)
    }
}
