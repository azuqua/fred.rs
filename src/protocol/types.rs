
use bytes::{
  BytesMut
};

use futures::sync::oneshot::{
  Sender as OneshotSender
};

use std::fmt;

use super::utils as protocol_utils;
use crate::types::*;
use crate::error::{
  RedisError,
  RedisErrorKind
};
use crate::utils as utils;

use crate::metrics;
use crate::metrics::{
  SizeStats
};
use std::sync::Arc;
use parking_lot::RwLock;
use std::ops::{
  Deref,
  DerefMut
};

pub const CR: char = '\r';
pub const LF: char = '\n';

pub const REDIS_CLUSTER_SLOTS: u16 = 16384;

#[cfg(not(feature="super-duper-bad-networking"))]
pub const MAX_COMMAND_ATTEMPTS: usize = 3;
#[cfg(feature="super-duper-bad-networking")]
pub const MAX_COMMAND_ATTEMPTS: usize = 20;

use redis_protocol::types::{
  FrameKind as ProtocolFrameKind,
  Frame as ProtocolFrame,
};

pub use redis_protocol::{
  CRLF,
  NULL,
  redis_keyslot
};

use crate::multiplexer::types::SplitCommand;
use futures::sync::mpsc::UnboundedSender;
use std::collections::{VecDeque, HashMap};
use std::rc::Rc;
use std::cell::RefCell;

#[derive(Clone)]
pub enum ResponseKind {
  Blocking {
    tx: Option<UnboundedSender<Frame>>
  },
  Multiple {
    count: usize,
    buffer: VecDeque<Frame>
  }
}

impl fmt::Debug for ResponseKind {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[Response Kind]")
  }
}

impl PartialEq for ResponseKind {
  fn eq(&self, other: &ResponseKind) -> bool {
    match *self {
      ResponseKind::Blocking {..} => match *other {
        ResponseKind::Blocking {..} => true,
        ResponseKind::Multiple {..} => false
      },
      ResponseKind::Multiple {..} => match *other {
        ResponseKind::Blocking {..} => false,
        ResponseKind::Multiple {..} => true
      }
    }
  }
}

impl Eq for ResponseKind {}

pub struct KeyScanInner {
  pub cursor: String,
  pub tx: UnboundedSender<Result<ScanResult, RedisError>>
}

impl PartialEq for KeyScanInner {
  fn eq(&self, other: &KeyScanInner) -> bool {
    self.cursor == other.cursor
  }
}

impl Eq for KeyScanInner {}

pub enum ValueScanResult {
  SScan(SScanResult),
  HScan(HScanResult),
  ZScan(ZScanResult)
}

pub struct ValueScanInner {
  pub cursor: String,
  pub tx: UnboundedSender<Result<ValueScanResult, RedisError>>
}

impl PartialEq for ValueScanInner {
  fn eq(&self, other: &ValueScanInner) -> bool {
    self.cursor == other.cursor
  }
}

impl Eq for ValueScanInner {}

impl ValueScanInner {

  pub fn transform_hscan_result(mut data: Vec<RedisValue>) -> Result<HashMap<RedisKey, RedisValue>, RedisError> {
    if data.is_empty() {
      return Ok(HashMap::new());
    }
    if data.len() % 2 != 0 {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid HSCAN result. Expected array with an even number of elements."
      ));
    }

    let mut out = HashMap::with_capacity(data.len() / 2);

    for mut chunk in data.chunks_exact_mut(2) {
      let key = match chunk[0].take() {
        RedisValue::String(s) => RedisKey::new(s),
        _ => return Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid HSCAN result. Expected redis key."
        ))
      };

      out.insert(key, chunk[1].take());
    }

    Ok(out)
  }

  pub fn transform_zscan_result(mut data: Vec<RedisValue>) -> Result<Vec<(RedisValue, f64)>, RedisError> {
    if data.is_empty() {
      return Ok(Vec::new());
    }
    if data.len() % 2 != 0 {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid ZSCAN result. Expected array with an even number of elements."
      ));
    }

    let mut out = Vec::with_capacity(data.len() / 2);

    for mut chunk in data.chunks_exact_mut(2) {
      let value = chunk[0].take();
      let score = match chunk[1].take() {
        RedisValue::String(s) => utils::redis_string_to_f64(&s)?,
        RedisValue::Integer(i) => i as f64,
        RedisValue::Null => return Err(RedisError::new(
          RedisErrorKind::ProtocolError, "Invalid HSCAN result. Expected a string or integer score."
        ))
      };

      out.push((value, score));
    }

    Ok(out)
  }

}

#[derive(Eq, PartialEq)]
pub enum RedisCommandKind {
  Append,
  Auth,
  BgreWriteAof,
  BgSave,
  BitCount,
  BitField,
  BitOp,
  BitPos,
  BlPop(ResponseKind),
  BrPop(ResponseKind),
  BrPopLPush(ResponseKind),
  ClientKill,
  ClientList,
  ClientGetName,
  ClientPause,
  ClientReply,
  ClientSetname,
  ClusterAddSlots,
  ClusterCountFailureReports,
  ClusterCountKeysInSlot,
  ClusterDelSlots,
  ClusterFailOver,
  ClusterForget,
  ClusterGetKeysInSlot,
  ClusterInfo,
  ClusterKeySlot,
  ClusterMeet,
  ClusterNodes,
  ClusterReplicate,
  ClusterReset,
  ClusterSaveConfig,
  ClusterSetConfigEpoch,
  ClusterSetSlot,
  ClusterSlaves,
  ClusterSlots,
  ConfigGet,
  ConfigRewrite,
  ConfigSet,
  ConfigResetStat,
  DBSize,
  Decr,
  DecrBy,
  Del,
  Discard,
  Dump,
  Echo,
  Eval,
  EvalSha,
  Exec,
  Exists,
  Expire,
  ExpireAt,
  FlushAll,
  FlushDB,
  GeoAdd,
  GeoHash,
  GeoPos,
  GeoDist,
  GeoRadius,
  GeoRadiusByMember,
  Get,
  GetBit,
  GetRange,
  GetSet,
  HDel,
  HExists,
  HGet,
  HGetAll,
  HIncrBy,
  HIncrByFloat,
  HKeys,
  HLen,
  HMGet,
  HMSet,
  HSet,
  HSetNx,
  HStrLen,
  HVals,
  Incr,
  IncrBy,
  IncrByFloat,
  Info,
  Keys,
  LastSave,
  LIndex,
  LInsert,
  LLen,
  LPop,
  LPush,
  LPushX,
  LRange,
  LRem,
  LSet,
  LTrim,
  Mget,
  Migrate,
  Monitor,
  Move,
  Mset,
  Msetnx,
  Multi,
  Object,
  Persist,
  Pexpire,
  Pexpireat,
  Pfadd,
  Pfcount,
  Pfmerge,
  Ping,
  Psetex,
  Psubscribe(ResponseKind),
  Pubsub,
  Pttl,
  Publish,
  Punsubscribe(ResponseKind),
  Quit,
  Randomkey,
  Readonly,
  Readwrite,
  Rename,
  Renamenx,
  Restore,
  Role,
  Rpop,
  Rpoplpush,
  Rpush,
  Rpushx,
  Sadd,
  Save,
  Scard,
  Sdiff,
  Sdiffstore,
  Select,
  Set,
  Setbit,
  Setex,
  Setnx,
  Setrange,
  Shutdown,
  Sinter,
  Sinterstore,
  Sismember,
  Slaveof,
  Slowlog,
  Smembers,
  Smove,
  Sort,
  Spop,
  Srandmember,
  Srem,
  Strlen,
  Subscribe,
  Sunion,
  Sunionstore,
  Swapdb,
  Sync,
  Time,
  Touch,
  Ttl,
  Type,
  Unsubscribe,
  Unlink,
  Unwatch,
  Wait,
  Watch,
  Zadd,
  Zcard,
  Zcount,
  Zincrby,
  Zinterstore,
  Zlexcount,
  Zrange,
  Zrangebylex,
  Zrangebyscore,
  Zrank,
  Zrem,
  Zremrangebylex,
  Zremrangebyrank,
  Zremrangebyscore,
  Zrevrange,
  Zrevrangebylex,
  Zrevrangebyscore,
  Zrevrank,
  Zscore,
  Zunionstore,
  Zpopmax,
  Zpopmin,
  Scan(KeyScanInner),
  Sscan(ValueScanInner),
  Hscan(ValueScanInner),
  Zscan(ValueScanInner),
  #[doc(hidden)]
  _Close,
  #[doc(hidden)]
  _Split(Option<SplitCommand>)
}

impl fmt::Debug for RedisCommandKind {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    if self.is_cluster_command() {
      write!(f, "{} {}", self.to_str(), self.cluster_args().unwrap_or("".into()))
    }else if self.is_client_command() {
      write!(f, "{} {}", self.to_str(), self.client_args().unwrap_or("".into()))
    }else {
      write!(f, "{}", self.to_str())
    }
  }
}

impl RedisCommandKind {

  pub fn is_scan(&self) -> bool {
    match *self {
      RedisCommandKind::Scan(_) => true,
      _ => false
    }
  }

  pub fn is_hscan(&self) -> bool {
    match *self {
      RedisCommandKind::Hscan(_) => true,
      _ => false
    }
  }

  pub fn is_sscan(&self) -> bool {
    match *self {
      RedisCommandKind::Sscan(_) => true,
      _ => false
    }
  }

  pub fn is_zscan(&self) -> bool {
    match *self {
      RedisCommandKind::Zscan(_) => true,
      _ => false
    }
  }

  pub fn is_value_scan(&self) -> bool {
    match *self {
      RedisCommandKind::Zscan(_)
        | RedisCommandKind::Hscan(_)
        | RedisCommandKind::Sscan(_) => true,
      _ => false
    }
  }

  pub fn has_response_kind(&self) -> bool {
    match *self {
      RedisCommandKind::Punsubscribe(_)
        | RedisCommandKind::Psubscribe(_)
        | RedisCommandKind::BlPop(_)
        | RedisCommandKind::BrPop(_)
        | RedisCommandKind::BrPopLPush(_) => true,
      _ => false
    }
  }

  pub fn has_multiple_response_kind(&self) -> bool {
    match *self {
      RedisCommandKind::Punsubscribe(_)
      | RedisCommandKind::Psubscribe(_) => true,
      _ => false
    }
  }

  pub fn has_blocking_response_kind(&self) -> bool {
    match *self {
      RedisCommandKind::BlPop(_)
      | RedisCommandKind::BrPop(_)
      | RedisCommandKind::BrPopLPush(_) => true,
      _ => false
    }
  }

  pub fn response_kind(&self) -> Option<&ResponseKind> {
    match *self {
      RedisCommandKind::Punsubscribe(ref k)
      | RedisCommandKind::Psubscribe(ref k)
      | RedisCommandKind::BlPop(ref k)
      | RedisCommandKind::BrPop(ref k)
      | RedisCommandKind::BrPopLPush(ref k) => Some(k),
      _ => None
    }
  }

  pub fn response_kind_mut(&mut self) -> Option<&mut ResponseKind> {
    match *self {
      RedisCommandKind::Punsubscribe(ref mut k)
      | RedisCommandKind::Psubscribe(ref mut k)
      | RedisCommandKind::BlPop(ref mut k)
      | RedisCommandKind::BrPop(ref mut k)
      | RedisCommandKind::BrPopLPush(ref mut k) => Some(k),
      _ => None
    }
  }

  pub fn is_split(&self) -> bool {
    match *self {
      RedisCommandKind::_Split(_) => true,
      _ => false
    }
  }

  pub fn is_close(&self) -> bool {
    match *self {
      RedisCommandKind::_Close => true,
      _ => false
    }
  }

  pub fn take_split(&mut self) -> Result<SplitCommand, RedisError> {
    match *self {
      RedisCommandKind::_Split(ref mut inner) => match inner.take() {
        Some(inner) => Ok(inner),
        None => Err(RedisError::new(
          RedisErrorKind::Unknown, "Missing split command options."
        ))
      },
      _ => Err(RedisError::new(
        RedisErrorKind::Unknown, "Invalid split command kind."
      ))
    }
  }

  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      RedisCommandKind::Append                          => "APPEND",
      RedisCommandKind::Auth                            => "AUTH",
      RedisCommandKind::BgreWriteAof                    => "BGREWRITEAOF",
      RedisCommandKind::BgSave                          => "BGSAVE",
      RedisCommandKind::BitCount                        => "BITCOUNT",
      RedisCommandKind::BitField                        => "BITFIELD",
      RedisCommandKind::BitOp                           => "BITOP",
      RedisCommandKind::BitPos                          => "BITPOS",
      RedisCommandKind::BlPop(_)                        => "BLPOP",
      RedisCommandKind::BrPop(_)                        => "BRPOP",
      RedisCommandKind::BrPopLPush(_)                   => "BRPOPLPUSH",
      RedisCommandKind::ClientKill                      => "CLIENT",
      RedisCommandKind::ClientList                      => "CLIENT",
      RedisCommandKind::ClientGetName                   => "CLIENT",
      RedisCommandKind::ClientPause                     => "CLIENT",
      RedisCommandKind::ClientReply                     => "CLIENT",
      RedisCommandKind::ClientSetname                   => "CLIENT",
      RedisCommandKind::ClusterAddSlots                 => "CLUSTER",
      RedisCommandKind::ClusterCountFailureReports      => "CLUSTER",
      RedisCommandKind::ClusterCountKeysInSlot          => "CLUSTER",
      RedisCommandKind::ClusterDelSlots                 => "CLUSTER",
      RedisCommandKind::ClusterFailOver                 => "CLUSTER",
      RedisCommandKind::ClusterForget                   => "CLUSTER",
      RedisCommandKind::ClusterGetKeysInSlot            => "CLUSTER",
      RedisCommandKind::ClusterInfo                     => "CLUSTER",
      RedisCommandKind::ClusterKeySlot                  => "CLUSTER",
      RedisCommandKind::ClusterMeet                     => "CLUSTER",
      RedisCommandKind::ClusterNodes                    => "CLUSTER",
      RedisCommandKind::ClusterReplicate                => "CLUSTER",
      RedisCommandKind::ClusterReset                    => "CLUSTER",
      RedisCommandKind::ClusterSaveConfig               => "CLUSTER",
      RedisCommandKind::ClusterSetConfigEpoch           => "CLUSTER",
      RedisCommandKind::ClusterSetSlot                  => "CLUSTER",
      RedisCommandKind::ClusterSlaves                   => "CLUSTER",
      RedisCommandKind::ClusterSlots                    => "CLUSTER",
      RedisCommandKind::ConfigGet                       => "CONFIG",
      RedisCommandKind::ConfigRewrite                   => "CONFIG",
      RedisCommandKind::ConfigSet                       => "CONFIG",
      RedisCommandKind::ConfigResetStat                 => "CONFIG",
      RedisCommandKind::DBSize                          => "DBSIZE",
      RedisCommandKind::Decr                            => "DECR",
      RedisCommandKind::DecrBy                          => "DECRBY",
      RedisCommandKind::Del                             => "DEL",
      RedisCommandKind::Discard                         => "DISCARD",
      RedisCommandKind::Dump                            => "DUMP",
      RedisCommandKind::Echo                            => "ECHO",
      RedisCommandKind::Eval                            => "EVAL",
      RedisCommandKind::EvalSha                         => "EVALSHA",
      RedisCommandKind::Exec                            => "EXEC",
      RedisCommandKind::Exists                          => "EXISTS",
      RedisCommandKind::Expire                          => "EXPIRE",
      RedisCommandKind::ExpireAt                        => "EXPIREAT",
      RedisCommandKind::FlushAll                        => "FLUSHALL",
      RedisCommandKind::FlushDB                         => "FLUSHDB",
      RedisCommandKind::GeoAdd                          => "GEOADD",
      RedisCommandKind::GeoHash                         => "GEOHASH",
      RedisCommandKind::GeoPos                          => "GEOPOS",
      RedisCommandKind::GeoDist                         => "GEODIST",
      RedisCommandKind::GeoRadius                       => "GEORADIUS",
      RedisCommandKind::GeoRadiusByMember               => "GEORADIUSBYMEMBER",
      RedisCommandKind::Get                             => "GET",
      RedisCommandKind::GetBit                          => "GETBIT",
      RedisCommandKind::GetRange                        => "GETRANGE",
      RedisCommandKind::GetSet                          => "GETSET",
      RedisCommandKind::HDel                            => "HDEL",
      RedisCommandKind::HExists                         => "HEXISTS",
      RedisCommandKind::HGet                            => "HGET",
      RedisCommandKind::HGetAll                         => "HGETALL",
      RedisCommandKind::HIncrBy                         => "HINCRBY",
      RedisCommandKind::HIncrByFloat                    => "HINCRBYFLOAT",
      RedisCommandKind::HKeys                           => "HKEYS",
      RedisCommandKind::HLen                            => "HLEN",
      RedisCommandKind::HMGet                           => "HMGET",
      RedisCommandKind::HMSet                           => "HMSET",
      RedisCommandKind::HSet                            => "HSET",
      RedisCommandKind::HSetNx                          => "HSETNX",
      RedisCommandKind::HStrLen                         => "HSTRLEN",
      RedisCommandKind::HVals                           => "HVALS",
      RedisCommandKind::Incr                            => "INCR",
      RedisCommandKind::IncrBy                          => "INCRBY",
      RedisCommandKind::IncrByFloat                     => "INCRBYFLOAT",
      RedisCommandKind::Info                            => "INFO",
      RedisCommandKind::Keys                            => "KEYS",
      RedisCommandKind::LastSave                        => "LASTSAVE",
      RedisCommandKind::LIndex                          => "LINDEX",
      RedisCommandKind::LInsert                         => "LINSERT",
      RedisCommandKind::LLen                            => "LLEN",
      RedisCommandKind::LPop                            => "LPOP",
      RedisCommandKind::LPush                           => "LPUSH",
      RedisCommandKind::LPushX                          => "LPUSHX",
      RedisCommandKind::LRange                          => "LRANGE",
      RedisCommandKind::LRem                            => "LREM",
      RedisCommandKind::LSet                            => "LSET",
      RedisCommandKind::LTrim                           => "LTRIM",
      RedisCommandKind::Mget                            => "MGET",
      RedisCommandKind::Migrate                         => "MIGRATE",
      RedisCommandKind::Monitor                         => "MONITOR",
      RedisCommandKind::Move                            => "MOVE",
      RedisCommandKind::Mset                            => "MSET",
      RedisCommandKind::Msetnx                          => "MSETNX",
      RedisCommandKind::Multi                           => "MULTI",
      RedisCommandKind::Object                          => "OBJECT",
      RedisCommandKind::Persist                         => "PERSIST",
      RedisCommandKind::Pexpire                         => "PEXPIRE",
      RedisCommandKind::Pexpireat                       => "PEXPIREAT",
      RedisCommandKind::Pfadd                           => "PFADD",
      RedisCommandKind::Pfcount                         => "PFCOUNT",
      RedisCommandKind::Pfmerge                         => "PFMERGE",
      RedisCommandKind::Ping                            => "PING",
      RedisCommandKind::Psetex                          => "PSETEX",
      RedisCommandKind::Psubscribe(_)                   => "PSUBSCRIBE",
      RedisCommandKind::Pubsub                          => "PUBSUB",
      RedisCommandKind::Pttl                            => "PTTL",
      RedisCommandKind::Publish                         => "PUBLISH",
      RedisCommandKind::Punsubscribe(_)                 => "PUNSUBSCRIBE",
      RedisCommandKind::Quit                            => "QUIT",
      RedisCommandKind::Randomkey                       => "RANDOMKEY",
      RedisCommandKind::Readonly                        => "READONLY",
      RedisCommandKind::Readwrite                       => "READWRITE",
      RedisCommandKind::Rename                          => "RENAME",
      RedisCommandKind::Renamenx                        => "RENAMENX",
      RedisCommandKind::Restore                         => "RESTORE",
      RedisCommandKind::Role                            => "ROLE",
      RedisCommandKind::Rpop                            => "RPOP",
      RedisCommandKind::Rpoplpush                       => "RPOPLPUSH",
      RedisCommandKind::Rpush                           => "RPUSH",
      RedisCommandKind::Rpushx                          => "RPUSHX",
      RedisCommandKind::Sadd                            => "SADD",
      RedisCommandKind::Save                            => "SAVE",
      RedisCommandKind::Scard                           => "SCARD",
      RedisCommandKind::Sdiff                           => "SDIFF",
      RedisCommandKind::Sdiffstore                      => "SDIFFSTORE",
      RedisCommandKind::Select                          => "SELECT",
      RedisCommandKind::Set                             => "SET",
      RedisCommandKind::Setbit                          => "SETBIT",
      RedisCommandKind::Setex                           => "SETEX",
      RedisCommandKind::Setnx                           => "SETNX",
      RedisCommandKind::Setrange                        => "SETRANGE",
      RedisCommandKind::Shutdown                        => "SHUTDOWN",
      RedisCommandKind::Sinter                          => "SINTER",
      RedisCommandKind::Sinterstore                     => "SINTERSTORE",
      RedisCommandKind::Sismember                       => "SISMEMBER",
      RedisCommandKind::Slaveof                         => "SLAVEOF",
      RedisCommandKind::Slowlog                         => "SLOWLOG",
      RedisCommandKind::Smembers                        => "SMEMBERS",
      RedisCommandKind::Smove                           => "SMOVE",
      RedisCommandKind::Sort                            => "SORT",
      RedisCommandKind::Spop                            => "SPOP",
      RedisCommandKind::Srandmember                     => "SRANDMEMBER",
      RedisCommandKind::Srem                            => "SREM",
      RedisCommandKind::Strlen                          => "STRLEN",
      RedisCommandKind::Subscribe                       => "SUBSCRIBE",
      RedisCommandKind::Sunion                          => "SUNION",
      RedisCommandKind::Sunionstore                     => "SUNIONSTORE",
      RedisCommandKind::Swapdb                          => "SWAPDB",
      RedisCommandKind::Sync                            => "SYNC",
      RedisCommandKind::Time                            => "TIME",
      RedisCommandKind::Touch                           => "TOUCH",
      RedisCommandKind::Ttl                             => "TTL",
      RedisCommandKind::Type                            => "TYPE",
      RedisCommandKind::Unsubscribe                     => "UNSUBSCRIBE",
      RedisCommandKind::Unlink                          => "UNLINK",
      RedisCommandKind::Unwatch                         => "UNWATCH",
      RedisCommandKind::Wait                            => "WAIT",
      RedisCommandKind::Watch                           => "WATCH",
      RedisCommandKind::Zadd                            => "ZADD",
      RedisCommandKind::Zcard                           => "ZCARD",
      RedisCommandKind::Zcount                          => "ZCOUNT",
      RedisCommandKind::Zincrby                         => "ZINCRBY",
      RedisCommandKind::Zinterstore                     => "ZINTERSTORE",
      RedisCommandKind::Zlexcount                       => "ZLEXCOUNT",
      RedisCommandKind::Zrange                          => "ZRANGE",
      RedisCommandKind::Zrangebylex                     => "ZRANGEBYLEX",
      RedisCommandKind::Zrangebyscore                   => "ZRANGEBYSCORE",
      RedisCommandKind::Zrank                           => "ZRANK",
      RedisCommandKind::Zrem                            => "ZREM",
      RedisCommandKind::Zremrangebylex                  => "ZREMRANGEBYLEX",
      RedisCommandKind::Zremrangebyrank                 => "ZREMRANGEBYRANK",
      RedisCommandKind::Zremrangebyscore                => "ZREMRANGEBYSCORE",
      RedisCommandKind::Zrevrange                       => "ZREVRANGE",
      RedisCommandKind::Zrevrangebylex                  => "ZREVRANGEBYLEX",
      RedisCommandKind::Zrevrangebyscore                => "ZREVRANGEBYSCORE",
      RedisCommandKind::Zrevrank                        => "ZREVRANK",
      RedisCommandKind::Zscore                          => "ZSCORE",
      RedisCommandKind::Zunionstore                     => "ZUNIONSTORE",
      RedisCommandKind::Zpopmax                         => "ZPOPMAX",
      RedisCommandKind::Zpopmin                         => "ZPOPMIN",
      RedisCommandKind::Scan(_)                         => "SCAN",
      RedisCommandKind::Sscan(_)                        => "SSCAN",
      RedisCommandKind::Hscan(_)                        => "HSCAN",
      RedisCommandKind::Zscan(_)                        => "ZSCAN",
      RedisCommandKind::_Close
        | RedisCommandKind::_Split(_)                    => panic!("unreachable (redis command)")
    }
  }

  pub fn is_cluster_command(&self) -> bool {
    match *self {
      RedisCommandKind::ClusterAddSlots
      | RedisCommandKind::ClusterCountFailureReports
      | RedisCommandKind::ClusterCountKeysInSlot
      | RedisCommandKind::ClusterDelSlots
      | RedisCommandKind::ClusterFailOver
      | RedisCommandKind::ClusterForget
      | RedisCommandKind::ClusterGetKeysInSlot
      | RedisCommandKind::ClusterInfo
      | RedisCommandKind::ClusterKeySlot
      | RedisCommandKind::ClusterMeet
      | RedisCommandKind::ClusterNodes
      | RedisCommandKind::ClusterReplicate
      | RedisCommandKind::ClusterReset
      | RedisCommandKind::ClusterSaveConfig
      | RedisCommandKind::ClusterSetConfigEpoch
      | RedisCommandKind::ClusterSetSlot
      | RedisCommandKind::ClusterSlaves
      | RedisCommandKind::ClusterSlots                 => true,
      _ => false
    }
  }

  pub fn cluster_args(&self) -> Option<String> {
    let s = match *self {
      RedisCommandKind::ClusterAddSlots                 => "ADDSLOTS",
      RedisCommandKind::ClusterCountFailureReports      => "COUNT-FAILURE-REPORTS",
      RedisCommandKind::ClusterCountKeysInSlot          => "COUNTKEYSINSLOT",
      RedisCommandKind::ClusterDelSlots                 => "DELSLOTS",
      RedisCommandKind::ClusterFailOver                 => "FAILOVER",
      RedisCommandKind::ClusterForget                   => "FORGET",
      RedisCommandKind::ClusterGetKeysInSlot            => "GETKEYSINSLOT",
      RedisCommandKind::ClusterInfo                     => "INFO",
      RedisCommandKind::ClusterKeySlot                  => "KEYSLOT",
      RedisCommandKind::ClusterMeet                     => "MEET",
      RedisCommandKind::ClusterNodes                    => "NODES",
      RedisCommandKind::ClusterReplicate                => "REPLICATE",
      RedisCommandKind::ClusterReset                    => "RESET",
      RedisCommandKind::ClusterSaveConfig               => "SAVECONFIG",
      RedisCommandKind::ClusterSetConfigEpoch           => "SET-CONFIG-EPOCH",
      RedisCommandKind::ClusterSetSlot                  => "SETSLOT",
      RedisCommandKind::ClusterSlaves                   => "SLAVES",
      RedisCommandKind::ClusterSlots                    => "SLOTS",
      _ => return None
    };

    Some(s.to_owned())
  }

  pub fn is_client_command(&self) -> bool {
    match *self {
      RedisCommandKind::ClientGetName
      | RedisCommandKind::ClientKill
      | RedisCommandKind::ClientList
      | RedisCommandKind::ClientPause
      | RedisCommandKind::ClientReply
      | RedisCommandKind::ClientSetname => true,
      _ => false
    }
  }

  pub fn client_args(&self) -> Option<String> {
    let s = match *self {
      RedisCommandKind::ClientKill                     => "KILL",
      RedisCommandKind::ClientList                     => "LIST",
      RedisCommandKind::ClientGetName                  => "GETNAME",
      RedisCommandKind::ClientPause                    => "PAUSE",
      RedisCommandKind::ClientReply                    => "REPLY",
      RedisCommandKind::ClientSetname                  => "SETNAME",
      _ => return None
    };

    Some(s.to_owned())
  }

  pub fn is_config_command(&self) -> bool {
    match *self {
      RedisCommandKind::ConfigGet
      | RedisCommandKind::ConfigRewrite
      | RedisCommandKind::ConfigSet
      | RedisCommandKind::ConfigResetStat            => true,
      _ => false
    }
  }

  pub fn config_args(&self) -> Option<String> {
    let s = match *self {
      RedisCommandKind::ConfigGet                       => "GET",
      RedisCommandKind::ConfigRewrite                   => "REWRITE",
      RedisCommandKind::ConfigSet                       => "SET",
      RedisCommandKind::ConfigResetStat                 => "RESETSTAT",
      _ => return None
    };

    Some(s.to_owned())
  }

  pub fn is_blocking(&self) -> bool {
    match *self {
      RedisCommandKind::BlPop(_)
      | RedisCommandKind::BrPop(_)
      | RedisCommandKind::BrPopLPush(_) => true,
      _ => false
    }
  }

  pub fn is_read(&self) -> bool {
    use RedisCommandKind::*;

    // TODO finish this and use for sending reads to slaves
    match *self {
      _ => false
    }
  }

}


/// Alias for a sender to notify the caller that a response was received.
pub type ResponseSender = Option<OneshotSender<Result<Frame, RedisError>>>;
/// Whether or not to refresh the cluster cache.
pub type RefreshCache = bool;

/// An arbitrary Redis command.
pub struct RedisCommand {
  pub kind: RedisCommandKind,
  pub args: Vec<RedisValue>,
  /// Sender for notifying the caller that a response was received.
  pub tx: ResponseSender,
  /// Number of times the request was sent to the server.
  pub attempted: usize
}

impl fmt::Debug for RedisCommand {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[RedisCommand Kind: {:?}, Args: {:?}]", &self.kind, &self.args)
  }
}

impl RedisCommand {

  pub fn new(kind: RedisCommandKind, args: Vec<RedisValue>, tx: ResponseSender) -> RedisCommand {
    RedisCommand {
      kind, args, tx,
      attempted: 0
    }
  }

  pub fn incr_attempted(&mut self) {
    self.attempted += 1;
  }

  pub fn max_attempts_exceeded(&self) -> bool {
    self.attempted >= MAX_COMMAND_ATTEMPTS
  }

  /// Convert to a single frame with an array of bulk strings (or null).
  ///
  /// This consumes the arguments array but does not take the response sender.
  pub fn to_frame(&self) -> Result<Frame, RedisError> {
    let mut bulk_strings: Vec<Frame> = Vec::with_capacity(self.args.len() + 1);

    let cmd = self.kind.to_str().as_bytes();
    bulk_strings.push(ProtocolFrame::BulkString(cmd.to_vec()));

    if let Some(frame) = protocol_utils::command_args(&self.kind) {
      bulk_strings.push(frame);
    }

    for value in self.args.iter() {
      let frame: Frame = match value {
        RedisValue::Integer(i) => ProtocolFrame::BulkString(i.to_string().as_bytes().to_vec()),
        RedisValue::String(s) => ProtocolFrame::BulkString(s.as_bytes().to_vec()),
        RedisValue::Null => ProtocolFrame::Null
      };

      bulk_strings.push(frame);
    }

    Ok(Frame::Array(bulk_strings))
  }

  /// Commands that do not need to run on a specific host in a cluster.
  pub fn no_cluster(&self) -> bool {
    match self.kind {
      RedisCommandKind::Publish
      | RedisCommandKind::Subscribe
      | RedisCommandKind::Unsubscribe
      | RedisCommandKind::Psubscribe(_)
      | RedisCommandKind::Punsubscribe(_)
      | RedisCommandKind::Ping
      | RedisCommandKind::Info
      | RedisCommandKind::Scan(_)
      | RedisCommandKind::FlushAll
      | RedisCommandKind::FlushDB => true,
      _ => false
    }
  }

  pub fn extract_key(&self) -> Option<&str> {
    if self.no_cluster() {
      return None;
    }

    match self.args.first() {
      Some(arg) => match *arg {
        RedisValue::String(ref s) => Some(s),
        _ => None
      },
      None => None
    }
  }

}


#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlaveNodes {
  servers: Vec<String>,
  next: usize
}

impl SlaveNodes {

  pub fn new(servers: Vec<String>) -> SlaveNodes {
    SlaveNodes {
      servers,
      next: 0
    }
  }

  pub fn add(&mut self, server: String) {
    self.servers.push(server);
  }

  pub fn clear(&mut self) {
    self.servers.clear();
    self.next = 0;
  }

  pub fn next(&mut self) -> Option<String> {
    if self.servers.len() == 0 { return None; }

    let last = self.next;
    self.next = (self.next + 1) % self.servers.len();

    self.servers.get(last).cloned()
  }

}

// Range contains is still only on nightly...
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SlotRange {
  pub start: u16,
  pub end: u16,
  pub server: String,
  pub id: String,
  // TODO
  // cache slaves for each master and round-robin reads to the slaves + master, and only send writes to the master
  pub slaves: Option<SlaveNodes>
}

#[derive(Debug, Clone)]
pub struct ClusterKeyCache {
  data: Vec<Arc<SlotRange>>
}

impl ClusterKeyCache {

  pub fn new(status: Option<String>) -> Result<ClusterKeyCache, RedisError> {
    let mut cache = ClusterKeyCache {
      data: Vec::new()
    };

    if let Some(status) = status {
      cache.rebuild(status)?;
    }

    Ok(cache)
  }

  pub fn clear(&mut self) {
    self.data.clear();
  }

  pub fn rebuild(&mut self, status: String) -> Result<(), RedisError> {
    let mut parsed = protocol_utils::parse_cluster_nodes(status)?;

    self.data.clear();
    for (_, ranges) in parsed.drain() {
      for slot in ranges {
        self.data.push(Arc::new(slot));
      }
    }

    self.data.sort_by(|lhs, rhs| {
      lhs.start.cmp(&rhs.start)
    });

    self.data.shrink_to_fit();
    Ok(())
  }

  pub fn get_server(&self, slot: u16) -> Option<Arc<SlotRange>> {
    protocol_utils::binary_search(&self.data, slot)
  }

  pub fn len(&self) -> usize {
    self.data.len()
  }

  pub fn slots(&self) -> &Vec<Arc<SlotRange>> {
    &self.data
  }

  pub fn random_slot(&self) -> Option<Arc<SlotRange>> {
    // for now just grab the first one, maybe in the future use a random slot.
    // at the very least if this starts causing errors it'll be easier to find due to this choice,
    // since debugging anything that depends on randomness is a nightmare
    if self.data.len() > 0 {
      // or change to use a round-robin approach
      Some(self.data[0].clone())
    }else{
      None
    }
  }

}
