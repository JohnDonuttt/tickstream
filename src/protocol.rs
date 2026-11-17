//! JSON messages exchanged over the WebSocket.
//!
//! Server to client:
//!
//! ```json
//! {"type":"hello","symbols":["BTC-USD","ETH-USD","SOL-USD"]}
//! {"type":"tick","symbol":"BTC-USD","price":67012.5,"seq":1234,"ts":1731840000000}
//! {"type":"subscribed","symbols":["BTC-USD"]}
//! {"type":"error","message":"unknown symbol: DOGE-USD"}
//! ```
//!
//! Client to server:
//!
//! ```json
//! {"type":"subscribe","symbols":["BTC-USD","ETH-USD"]}
//! {"type":"subscribe"}
//! ```
//!
//! A `subscribe` replaces the client's current filter. Leaving out `symbols`
//! means "everything". New connections start subscribed to everything.

use serde::{Deserialize, Serialize};

use crate::ticker::Tick;

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ServerMessage<'a> {
    Hello { symbols: &'a [&'static str] },
    Tick(&'a Tick),
    Subscribed { symbols: Vec<&'static str> },
    Error { message: String },
}

impl ServerMessage<'_> {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("server message serialises")
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum ClientMessage {
    Subscribe {
        #[serde(default)]
        symbols: Option<Vec<String>>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_message_is_flat_with_type_tag() {
        let tick = Tick {
            symbol: "BTC-USD",
            price: 1.5,
            seq: 3,
            ts: 10,
        };
        let v: serde_json::Value =
            serde_json::from_str(&ServerMessage::Tick(&tick).to_json()).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"type":"tick","symbol":"BTC-USD","price":1.5,"seq":3,"ts":10})
        );
    }

    #[test]
    fn hello_lists_symbols() {
        let json = ServerMessage::Hello {
            symbols: &["A", "B"],
        }
        .to_json();
        assert_eq!(json, r#"{"type":"hello","symbols":["A","B"]}"#);
    }

    #[test]
    fn parses_subscribe_with_and_without_symbols() {
        let some: ClientMessage =
            serde_json::from_str(r#"{"type":"subscribe","symbols":["ETH-USD"]}"#).unwrap();
        assert_eq!(
            some,
            ClientMessage::Subscribe {
                symbols: Some(vec!["ETH-USD".into()])
            }
        );
        let all: ClientMessage = serde_json::from_str(r#"{"type":"subscribe"}"#).unwrap();
        assert_eq!(all, ClientMessage::Subscribe { symbols: None });
    }

    #[test]
    fn rejects_unknown_messages() {
        assert!(serde_json::from_str::<ClientMessage>(r#"{"type":"buy"}"#).is_err());
        assert!(serde_json::from_str::<ClientMessage>(r#"{"symbols":[]}"#).is_err());
        assert!(serde_json::from_str::<ClientMessage>("not json").is_err());
    }
}
