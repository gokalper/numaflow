use crate::message::Message;

/// Enum to represent the different types of
#[derive(Debug, Clone)]
#[allow(dead_code)] // Future: Phase 4 multi-sink routing (FbSink, OsSink)
pub enum MessageToSink {
    Primary(Message),
    Fallback(Message),
    OnSuccess(Message),
}
