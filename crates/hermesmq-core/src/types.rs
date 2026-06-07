use serde::{Deserialize, Serialize};

pub type NodeId = u64;

pub type Offset = u64;

pub type LeaseId = u64;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TopicId(pub String);

impl From<&str> for TopicId {
    fn from(s: &str) -> Self {
        TopicId(s.to_string())
    }
}

impl std::fmt::Display for TopicId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GroupId(pub String);

impl From<&str> for GroupId {
    fn from(s: &str) -> Self {
        GroupId(s.to_string())
    }
}

impl std::fmt::Display for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId {
    pub topic: TopicId,
    pub offset: Offset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub struct Priority(pub u8);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ContentType {
    #[default]
    Raw,
    Text,
    Json,
    MsgPack,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckMode(pub AckModeKind);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum AckModeKind {
    #[default]
    Manual,
    Auto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub offset: Offset,
    pub priority: Priority,
    pub content_type: ContentType,
    pub payload: Vec<u8>,
    pub ts_ms: u64,
}
