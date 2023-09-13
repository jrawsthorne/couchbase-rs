use crate::{failover_table::FailoverTable, hash_table::HashTable};
use crossbeam_utils::atomic::AtomicCell;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serializer};
use std::{
    fmt::{self, Display},
    ops::Rem,
    str::FromStr,
    sync::Arc,
};

#[derive(Debug)]
pub struct VBucket {
    pub id: Vbid,
    pub hash_table: Mutex<HashTable>,
    state: AtomicCell<State>,
    _failover_table: FailoverTable,
    // Can state just be inside the mutex??
    state_lock: Mutex<()>,
}

impl VBucket {
    pub fn new(id: Vbid, state: State, failover_table: FailoverTable) -> Self {
        Self {
            id,
            hash_table: Mutex::new(Default::default()),
            state: AtomicCell::new(state),
            _failover_table: failover_table,
            state_lock: Mutex::new(()),
        }
    }

    pub fn state(&self) -> State {
        self.state.load()
    }

    pub fn get_state_lock(&self) -> MutexGuard<'_, ()> {
        self.state_lock.lock()
    }

    pub fn set_state(&self, state: State) {
        let _guard = self.get_state_lock();
        self.set_state_unlocked(state);
    }

    fn set_state_unlocked(&self, state: State) {
        self.state.store(state);
    }
}

pub type VBucketPtr = Arc<VBucket>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Vbid(u16);

impl Vbid {
    pub fn new(id: u16) -> Self {
        Self(id)
    }
}

impl From<Vbid> for usize {
    fn from(vbid: Vbid) -> Self {
        vbid.0 as usize
    }
}

impl From<Vbid> for u16 {
    fn from(vbid: Vbid) -> Self {
        vbid.0
    }
}

impl From<usize> for Vbid {
    fn from(id: usize) -> Self {
        Self(id as u16)
    }
}

impl From<u16> for Vbid {
    fn from(id: u16) -> Self {
        Self(id)
    }
}

impl Display for Vbid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Rem<u16> for Vbid {
    type Output = u16;

    fn rem(self, rhs: u16) -> Self::Output {
        self.0 % rhs
    }
}

// Note that integers are stored as strings to avoid any undesired
// rounding (JSON in general only guarantees ~2^53 precision on integers).
// While serde _does_ support full 64bit precision for integers,
// let's not rely on that for all future uses.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VBucketState {
    #[serde(
        serialize_with = "serialize_num_as_str",
        deserialize_with = "deserialize_num_as_str"
    )]
    pub max_deleted_seqno: u64,

    #[serde(skip_deserializing, skip_serializing)]
    pub high_seqno: i64,

    #[serde(skip_deserializing, skip_serializing)]
    pub purge_seqno: u64,

    #[serde(
        serialize_with = "serialize_num_as_str",
        deserialize_with = "deserialize_num_as_str"
    )]
    pub snap_start: u64,

    #[serde(
        serialize_with = "serialize_num_as_str",
        deserialize_with = "deserialize_num_as_str"
    )]
    pub snap_end: u64,

    #[serde(
        serialize_with = "serialize_num_as_str",
        deserialize_with = "deserialize_num_as_str"
    )]
    pub max_cas: u64,

    #[serde(
        serialize_with = "serialize_num_as_str",
        deserialize_with = "deserialize_num_as_str"
    )]
    pub hlc_epoch: i64,

    pub might_contain_xattrs: bool,

    pub namespaces_supported: bool,

    pub version: usize,

    #[serde(
        serialize_with = "serialize_num_as_str",
        deserialize_with = "deserialize_num_as_str"
    )]
    pub completed_seqno: u64,

    #[serde(
        serialize_with = "serialize_num_as_str",
        deserialize_with = "deserialize_num_as_str"
    )]
    pub prepared_seqno: u64,

    #[serde(
        serialize_with = "serialize_num_as_str",
        deserialize_with = "deserialize_num_as_str"
    )]
    pub high_prepared_seqno: u64,

    #[serde(
        serialize_with = "serialize_num_as_str",
        deserialize_with = "deserialize_num_as_str"
    )]
    pub max_visible_seqno: u64,

    #[serde(
        serialize_with = "serialize_num_as_str",
        deserialize_with = "deserialize_num_as_str"
    )]
    pub on_disk_prepares: u64,

    #[serde(
        serialize_with = "serialize_num_as_str",
        deserialize_with = "deserialize_num_as_str"
    )]
    pub on_disk_prepare_bytes: u64,

    pub checkpoint_type: CheckpointType,

    pub state: State,

    pub failover_table: serde_json::Value,

    pub replication_topology: serde_json::Value,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CheckpointType {
    #[default]
    Memory,
    Disk,
    InitialDisk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Active,
    Replica,
    Pending,
    Dead,
}

fn serialize_num_as_str<S>(x: impl ToString, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&x.to_string())
}

fn deserialize_num_as_str<'de, T, D>(d: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: FromStr + Display,
    <T as FromStr>::Err: Display,
{
    let s = String::deserialize(d)?;
    s.parse().map_err(serde::de::Error::custom)
}
