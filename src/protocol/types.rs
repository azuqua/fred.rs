#![allow(dead_code)]

use bytes::{
  BytesMut
};

use futures::sync::oneshot::{
  Sender as OneshotSender
};

use tokio_io::codec::{
  Encoder,
  Decoder
};

use std::rc::Rc;
use std::fmt;

use super::utils as protocol_utils;
use super::super::types::*;
use super::super::error::{
  RedisError,
  RedisErrorKind
};

use ::metrics;
use ::metrics::{
  SizeTracker
};
use std::sync::Arc;
use parking_lot::RwLock;
use std::ops::{
  DerefMut
};

pub const CR: char = '\r';
pub const LF: char = '\n';
pub const NULL: &'static str = "$-1\r\n";

pub const REDIS_CLUSTER_SLOTS: u16 = 16384;

#[derive(Clone)]
pub struct SplitCommand {
  pub tx: Arc<RwLock<Option<OneshotSender<Result<Vec<RedisConfig>, RedisError>>>>>,
  pub key: Option<String>
}

impl SplitCommand {

  pub fn take(&mut self) -> (Option<OneshotSender<Result<Vec<RedisConfig>, RedisError>>>, Option<String>) {
    let mut tx_guard = self.tx.write();
    (tx_guard.deref_mut().take(), self.key.take())
  }

}

impl fmt::Debug for SplitCommand {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[SplitCommand]")
  }
}

impl PartialEq for SplitCommand {
  fn eq(&self, other: &SplitCommand) -> bool {
    self.key == other.key
  }
}

impl Eq for SplitCommand {}


#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RedisCommandKind {
  Append,
  Auth,
  BgreWriteAof,
  BgSave,
  BitCount,
  BitField,
  BitOp,
  BitPos,
  BlPop,
  BrPop,
  BrPopLPush,
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
  Psubscribed,
  Pubsub,
  Pttl,
  Publish,
  Punsubscribe,
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
  Zunionscore,
  Zunionstore,
  Scan,
  Sscan,
  Hscan,
  Zscan,
  #[doc(hidden)]
  _OnMessage,
  #[doc(hidden)]
  _Close,
  #[doc(hidden)]
  _Split(Option<SplitCommand>)
}

impl RedisCommandKind {

  pub fn is_split(&self) -> bool {
    match *self {
      RedisCommandKind::_Split(_) => true,
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

  fn to_string(&self) -> &'static str {
    match *self {
      RedisCommandKind::Append                          => "APPEND",
      RedisCommandKind::Auth                            => "AUTH",
      RedisCommandKind::BgreWriteAof                    => "BGREWRITEAOF",
      RedisCommandKind::BgSave                          => "BGSAVE",
      RedisCommandKind::BitCount                        => "BITCOUNT",
      RedisCommandKind::BitField                        => "BITFIELD",
      RedisCommandKind::BitOp                           => "BITOP",
      RedisCommandKind::BitPos                          => "BITPOS",
      RedisCommandKind::BlPop                           => "BLPOP",
      RedisCommandKind::BrPop                           => "BRPOP",
      RedisCommandKind::BrPopLPush                      => "BRPOPLPUSH",
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
      RedisCommandKind::Psubscribed                     => "PSUBSCRIBED",
      RedisCommandKind::Pubsub                          => "PUBSUB",
      RedisCommandKind::Pttl                            => "PTTL",
      RedisCommandKind::Publish                         => "PUBLISH",
      RedisCommandKind::Punsubscribe                    => "PUNSUBSCRIBE",
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
      RedisCommandKind::Zunionscore                     => "ZUNIONSCORE",
      RedisCommandKind::Zunionstore                     => "ZUNIONSTORE",
      RedisCommandKind::Scan                            => "SCAN",
      RedisCommandKind::Sscan                           => "SSCAN",
      RedisCommandKind::Hscan                           => "HSCAN",
      RedisCommandKind::Zscan                           => "ZSCAN",
      RedisCommandKind::_Close
        | RedisCommandKind::_OnMessage
        | RedisCommandKind::_Split(_)                   => panic!("unreachable")
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
      RedisCommandKind::BlPop 
        | RedisCommandKind::BrPop
        | RedisCommandKind::BrPopLPush => true,
      _ => false
    }
  }

  pub fn is_read(&self) -> bool {
    // TODO finish this
    match *self {
      RedisCommandKind::Get
        | RedisCommandKind::HGet
        | RedisCommandKind::Exists
        | RedisCommandKind::HExists => true,
      _ => false
    }
  }

}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FrameKind {
  SimpleString,
  Error,
  Integer,
  BulkString,
  Array,
  Moved,
  Ask,
  Null,
  Canceled
}

impl FrameKind {

  pub fn from_byte(d: u8) -> Option<FrameKind> {
    match d as char {
      '+' => Some(FrameKind::SimpleString),
      '-' => Some(FrameKind::Error),
      ':' => Some(FrameKind::Integer),
      '$' => Some(FrameKind::BulkString),
      '*' => Some(FrameKind::Array),
      _   => None
    }
  }

  pub fn to_byte(&self) -> u8 {
    match *self {
      FrameKind::SimpleString => '+' as u8,
      FrameKind::Error 
        | FrameKind::Moved
        | FrameKind::Ask      
        | FrameKind::Canceled => '-' as u8,
      FrameKind::Integer      => ':' as u8,
      FrameKind::BulkString 
        | FrameKind::Null     => '$' as u8,
      FrameKind::Array        => '*' as u8,
    }
  }

}

// padding CRLF is removed
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Frame {
  SimpleString(String),
  Error(String),
  Integer(i64),
  BulkString(Vec<u8>),
  Array(Vec<Frame>),
  Moved(String),
  Ask(String),
  Null,
  // used to gracefully close streams
  Canceled
}

impl Frame {

  pub fn into_error(self) -> Option<RedisError> {
    match self {
      Frame::Canceled => Some(RedisError::new_canceled()),
      Frame::Error(s) => Some(protocol_utils::better_error(s)),
      _ => None
    }
  }

  pub fn is_canceled(&self) -> bool {
    self.kind() == FrameKind::Canceled
  }

  pub fn is_error(&self) -> bool {
    match self.kind() {
      FrameKind::Error
        | FrameKind::Canceled => true,
      _ => false
    }
  }

  pub fn is_pubsub_message(&self) -> bool {
    flame_start!("redis:is_pubsub_message");
    let res = if let Frame::Array(ref frames) = *self {
      frames.len() == 3 
        && frames[0].kind() == FrameKind::BulkString 
        && frames[0].to_string().unwrap_or(String::new()) == "message" 
    }else{
      false
    };

    flame_end!("redis:is_pubsub_message");
    res
  }

  pub fn kind(&self) -> FrameKind {
    match *self {
      Frame::SimpleString(_) => FrameKind::SimpleString,
      Frame::Error(_)        => FrameKind::Error,
      Frame::Integer(_)      => FrameKind::Integer,
      Frame::BulkString(_)   => FrameKind::BulkString,
      Frame::Array(_)        => FrameKind::Array,
      Frame::Moved(_)        => FrameKind::Moved,
      Frame::Ask(_)          => FrameKind::Ask,
      Frame::Null            => FrameKind::Null,
      Frame::Canceled        => FrameKind::Canceled
    }
  }

  // mostly used for ClusterNodes command
  pub fn to_string(&self) -> Option<String> {
    flame_start!("redis:to_string");
    let res = match *self {
      Frame::SimpleString(ref s) => Some(s.clone()),
      Frame::BulkString(ref b) => {
        match String::from_utf8(b.to_vec()) {
          Ok(s) => Some(s),
          Err(_) => None
        }
      },
      _ => None
    };

    flame_end!("redis:to_string");
    res
  }

  pub fn into_results(self) -> Result<Vec<RedisValue>, RedisError> {
    flame_start!("redis:into_results");

    let res = match self {
      Frame::SimpleString(s) => Ok(vec![s.into()]),
      Frame::Integer(i) => Ok(vec![i.into()]),
      Frame::BulkString(b) => {
        match String::from_utf8(b) {
          Ok(s) => Ok(vec![s.into()]),
          Err(e) => Err(e.into())
        }
      },
      Frame::Array(mut frames) => {
        let mut out = Vec::with_capacity(frames.len());
        for frame in frames.drain(..) {
          // there shouldn't be errors buried in arrays...
          let mut res = frame.into_results()?;
          if res.len() > 1 {
            // nor should there be more than one layer of nested arrays
            return Err(RedisError::new(
              RedisErrorKind::ProtocolError, "Invalid nested array."
            ));
          }else if res.len() == 0 {
            // shouldn't be possible...
            return Err(RedisError::new(
              RedisErrorKind::Unknown, "Invalid empty frame."
            ));
          }

          out.push(res.pop().unwrap())
        }

        Ok(out)
      },
      Frame::Null => Ok(vec![RedisValue::Null]),
      Frame::Error(s) => Err(protocol_utils::better_error(s)),
      _ => Err(RedisError::new(
        RedisErrorKind::ProtocolError, "Invalid frame."
      ))
    };

    flame_end!("redis:into_results");
    res
  }

  pub fn into_single_result(self) -> Result<RedisValue, RedisError> {
    flame_start!("redis:into_single_result");

    let mut results = match self.into_results() {
      Ok(r) => r,
      Err(e) => {
        flame_end!("redis:into_single_result");
        return Err(e);
      }
    };

    if results.len() != 1 {
      flame_end!("redis:into_single_result");
      return Err(RedisError::new(RedisErrorKind::ProtocolError, "Invalid results.")) 
    }

    let out = results.pop().unwrap();
    flame_end!("redis:into_single_result");
    Ok(out)
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
      servers: servers,
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
  data: Vec<Rc<SlotRange>>
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
        self.data.push(Rc::new(slot));
      }
    }
    
    self.data.sort_by(|lhs, rhs| {
      lhs.start.cmp(&rhs.start)
    });

    self.data.shrink_to_fit();
    Ok(())
  }

  pub fn get_server(&self, slot: u16) -> Option<Rc<SlotRange>> {
    protocol_utils::binary_search(&self.data, slot)
  }
 
  pub fn len(&self) -> usize {
    self.data.len()
  }

  pub fn random_slot(&self) -> Option<Rc<SlotRange>> {
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

/// Alias for a sender to notify the caller that a response was received.
pub type ResponseSender = Option<OneshotSender<Result<Frame, RedisError>>>;
/// Whether or not to refresh the cluster cache.
pub type RefreshCache = bool;

pub struct RedisCommand {
  pub kind: RedisCommandKind, 
  pub args: Vec<RedisValue>,
  /// Sender for notifying the caller that a response was received.
  pub tx: ResponseSender
}

impl fmt::Debug for RedisCommand {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[RedisCommand Kind: {:?}, Args: {:?}]", &self.kind, &self.args)
  }
}

impl RedisCommand {

  pub fn new(kind: RedisCommandKind, args: Vec<RedisValue>, tx: ResponseSender) -> RedisCommand {
    RedisCommand {
      kind: kind,
      args: args,
      tx: tx
    }
  }
  
  // Convert to a single frame with an array of bulk strings (or null).
  // This consumes the arguments array
  pub fn to_frame(&mut self) -> Result<Frame, RedisError> {
    flame_start!("redis:to_frame");
    let mut bulk_strings: Vec<Frame> = Vec::with_capacity(self.args.len() + 1);

    let cmd = self.kind.to_string().as_bytes();
    bulk_strings.push(Frame::BulkString(cmd.to_vec()));

    if let Some(frame) = protocol_utils::command_args(&self.kind) {
      bulk_strings.push(frame);
    }
    
    for value in self.args.drain(..) {
      let frame: Frame = match value {
        RedisValue::Integer(i) => {
          Frame::BulkString(i.to_string().into_bytes())
        },
        RedisValue::String(s) => {
          Frame::BulkString(s.into_bytes())
        },
        RedisValue::Null => {
          Frame::Null
        }, 
        _ => return Err(RedisError::new(
          RedisErrorKind::ProtocolError, format!("Unencodable redis value: {:?}.", value.kind())
        ))
      };

      bulk_strings.push(frame);
    }

    flame_end!("redis:to_frame");
    Ok(Frame::Array(bulk_strings))
  }

  /// Commands that do not need to run on a specific host in a cluster.
  pub fn no_cluster(&self) -> bool {
    match self.kind {
      RedisCommandKind::Publish 
        | RedisCommandKind::Subscribe
        | RedisCommandKind::Unsubscribe
        | RedisCommandKind::Ping
        | RedisCommandKind::Info
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

pub struct RedisCodec {
  pub max_size: Option<usize>,
  pub size_stats: Arc<RwLock<SizeTracker>>
}

impl RedisCodec {

  pub fn new(max_size: Option<usize>, size_stats: Arc<RwLock<SizeTracker>>) -> RedisCodec {
    RedisCodec { max_size, size_stats, }
  }

}

impl Encoder for RedisCodec {
  type Item = Frame;
  type Error = RedisError;

  fn encode(&mut self, mut msg: Frame, buf: &mut BytesMut) -> Result<(), RedisError> {
    let _guard = flame_start!("redis:encode");
    let res = protocol_utils::frames_to_bytes(&mut msg, buf);

    metrics::sample_size(&self.size_stats, buf.len() as u64);
    res
  }

}

impl Decoder for RedisCodec {
  type Item = Frame;
  type Error = RedisError;

  fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Frame>, RedisError> {
    let _guard = flame_start!("redis:decode");
    trace!("Recv {:?} bytes.", buf.len());

    if buf.len() < 1 || !protocol_utils::ends_with_crlf(buf) {
      return Ok(None)
    }

    let result = protocol_utils::bytes_to_frames(buf, &self.max_size);
    let res = match result {
      Ok(inner) => match inner {
        Some((frame, size)) => {
          trace!("Parsed {:?} bytes.", size);
          metrics::sample_size(&self.size_stats, size as u64);

          buf.clear();
          Ok(Some(frame))
        },
        None => Ok(None)
      },
      Err(e) => {
        buf.clear();
        Err(e)
      }
    };

    res
  }

}

#[cfg(test)]
mod test{
  #![allow(unused_imports)]
  use super::*;


}





