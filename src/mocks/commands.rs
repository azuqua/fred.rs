use crate::types::*;
use crate::error::{RedisError, RedisErrorKind};
use redis_protocol::types::*;

use super::utils;
use super::types::*;
use std::time::{Instant, Duration};

use std::rc::Rc;

use std::collections::BTreeMap;

use crate::protocol::types::*;
use std::ops::{DerefMut, Deref};
use std::sync::Arc;


pub fn log_unimplemented(command: &RedisCommand) -> Result<Frame, RedisError> {
  Err(RedisError::new(
    RedisErrorKind::InvalidCommand,
    format!("Unimplemented redis command {} in mocking layer.", command.kind.to_str())
  ))
}

pub fn auth(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  utils::ok()
}

pub fn select(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  utils::ok()
}

pub fn set(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  // [key, value, [ex|px, count], nx|xx]
  args.reverse();

  let key = match args.pop() {
    Some(RedisValue::String(s)) => s,
    Some(RedisValue::Integer(i)) => i.to_string(),
    _ => return utils::null()
  };
  let key = utils::get_key(&*data, key);

  let value = match args.pop() {
    Some(v) => match v.kind() {
      RedisValueKind::Integer
      | RedisValueKind::String => v,
      _ => return Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid value."
      ))
    },
    _ => return Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Missing value."
    ))
  };

  if let Some(key_type) = data.key_types.get(&key) {
    if *key_type != KeyType::Data {
      return Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid data type value."
      ));
    }
  }

  if args.len() == 3 {
    // has expiration and nx|xx flag
    let mult = match args.pop() {
      Some(RedisValue::String(s)) => match s.as_ref() {
        "EX" => 1000,
        "PX" => 1,
        _ => unreachable!()
      },
      _ => return Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid expiration flag."
      ))
    };
    let count = match args.pop() {
      Some(RedisValue::Integer(i)) => i,
      Some(RedisValue::String(s)) => utils::to_int(&s)?,
      _ => return Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid expiration value."
      ))
    };
    let count = count * mult;

    let flag = match args.pop() {
      Some(RedisValue::String(s)) => match s.as_ref() {
        "NX" => SetOptions::NX,
        "XX" => SetOptions::XX,
        _ => unreachable!()
      },
      _ => return Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid set options NX|XX value."
      ))
    };

    if utils::should_set(&*data, &key, flag) {
      data.keys.insert(key.clone());
      data.key_types.insert(key.clone(), KeyType::Data);

      let now = Instant::now();
      let _ = data.expirations.write().deref_mut().add(&key, ExpireLog {
        after: now + Duration::from_millis(count as u64),
        internal: Some((now, (key.clone())))
      })?;

      data.data.insert(key, value);

      utils::ok()
    }else{

      utils::null()
    }
  }else if args.len() == 2 {
    // has expiration
    let mult = match args.pop() {
      Some(RedisValue::String(s)) => match s.as_ref() {
        "EX" => 1000,
        "PX" => 1,
        _ => unreachable!()
      },
      _ => return Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid expiration flag."
      ))
    };
    let count = match args.pop() {
      Some(RedisValue::Integer(i)) => i,
      Some(RedisValue::String(s)) => utils::to_int(&s)?,
      _ => return Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid expiration value."
      ))
    };
    let count = count * mult;

    data.keys.insert(key.clone());
    data.key_types.insert(key.clone(), KeyType::Data);

    let now = Instant::now();
    let _ = data.expirations.write().deref_mut().add(&key, ExpireLog {
      after: now + Duration::from_millis(count as u64),
      internal: Some((now, (key.clone())))
    })?;

    data.data.insert(key, value);

    utils::ok()
  }else if args.len() == 1 {
    // has nx|xx flag
    let flag = match args.pop() {
      Some(RedisValue::String(s)) => match s.as_ref() {
        "NX" => SetOptions::NX,
        "XX" => SetOptions::XX,
        _ => unreachable!()
      },
      _ => return Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid set options NX|XX value."
      ))
    };

    if utils::should_set(&*data, &key, flag) {
      data.keys.insert(key.clone());
      data.key_types.insert(key.clone(), KeyType::Data);

      data.data.insert(key, value);

      utils::ok()
    }else{

      utils::null()
    }
  }else{
    // has neither
    data.keys.insert(key.clone());
    data.key_types.insert(key.clone(), KeyType::Data);

    data.data.insert(key, value);
    utils::ok()
  }
}

pub fn get(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  args.reverse();

  let key = match args.pop() {
    Some(RedisValue::String(s)) => s,
    Some(RedisValue::Integer(i)) => i.to_string(),
    _ => return utils::null()
  };
  let key = utils::get_key(&*data, key);

  let val = match data.data.get(&key) {
    Some(&RedisValue::String(ref s)) => Frame::BulkString(s
      .to_owned()
      .into_bytes()),
    Some(&RedisValue::Integer(i)) => Frame::Integer(i.clone()),
    Some(&RedisValue::Null) | None => Frame::Null,
    _ => return Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Invalid key type."
    ))
  };

  Ok(val)
}

pub fn del(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  let keys: Vec<(KeyType, Arc<RedisKey>)> = args.into_iter().filter_map(|s| {
    let k = match s {
      RedisValue::String(s) => s,
      RedisValue::Integer(i) => i.to_string(),
      _ => return None
    };
    let k = utils::get_key(&*data, k);

    let kind = match data.key_types.get(&k) {
      Some(kind) => kind.clone(),
      None => return None
    };

    Some((kind, k))
  })
  .collect();

  let mut deleted = 0;
  for (kind, key) in keys.into_iter() {
    if data.keys.remove(&key) {
      deleted += 1;
    }
    let _ = data.key_types.remove(&key);
    let _ = data.expirations.write().deref_mut().del(&key);

    match kind {
      KeyType::Data => { data.data.remove(&key); },
      KeyType::Map => { data.maps.remove(&key); },
      KeyType::Set => { data.sets.remove(&key); },
      _ => {}
    };
  }

  Ok(Frame::Integer(deleted))
}

pub fn expire(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  args.reverse();

  let key = match args.pop() {
    Some(RedisValue::String(s)) => s,
    Some(RedisValue::Integer(i)) => i.to_string(),
    _ => return utils::null()
  };
  let key = utils::get_key(&*data, key);

  let seconds = match args.pop() {
    Some(RedisValue::String(s)) => utils::to_int(&s)?,
    Some(RedisValue::Integer(i)) => i.clone(),
    _ => return Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Invalid count."
    ))
  };
  let ms = seconds * 1000;

  if let Some(ref key) = data.keys.get(&key) {
    let now = Instant::now();
    let _key = key.clone();

    let _ = data.expirations.write().deref_mut().add(&_key, ExpireLog {
      after: now + Duration::from_millis(ms as u64),
      internal: Some((now, (_key.clone())))
    })?;

    Ok(Frame::Integer(1))
  }else{
    Ok(Frame::Integer(0))
  }
}

pub fn persist(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  args.reverse();

  let key = match args.pop() {
    Some(RedisValue::String(s)) => s,
    Some(RedisValue::Integer(i)) => i.to_string(),
    _ => return utils::null()
  };
  let key = utils::get_key(&*data, key);

  let count = data.expirations.write().deref_mut().del(&key)?;
  Ok(Frame::Integer(count as i64))
}

pub fn hget(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  args.reverse();

  let key = match args.pop() {
    Some(RedisValue::String(s)) => s,
    Some(RedisValue::Integer(i)) => i.to_string(),
    _ => return utils::null()
  };
  let key = utils::get_key(&*data, key);

  if let Some(key_type) = data.key_types.get(&key) {
    if *key_type != KeyType::Map {
      return Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid value data type."
      ));
    }
  }else{
    return utils::null();
  }

  let inner = match data.maps.get(&key) {
    Some(i) => i,
    None => return utils::null()
  };

  let field = match args.pop() {
    Some(RedisValue::String(s)) => s,
    Some(RedisValue::Integer(i)) => i.to_string(),
    _ => return Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Invalid field."
    ))
  };
  let field: RedisKey = field.into();

  let val = match inner.get(&field) {
    Some(&RedisValue::String(ref s)) => Frame::BulkString(s
      .to_owned()
      .into_bytes()),
    Some(&RedisValue::Integer(i)) => Frame::Integer(i.clone()),
    Some(&RedisValue::Null) | None => Frame::Null,
    _ => return Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Invalid key type."
    ))
  };

  Ok(val)
}

pub fn hset(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  args.reverse();

  let key = match args.pop() {
    Some(RedisValue::String(s)) => s,
    Some(RedisValue::Integer(i)) => i.to_string(),
    _ => return Ok(Frame::Integer(0))
  };
  let key = utils::get_key(&*data, key);

  if let Some(key_type) = data.key_types.get(&key) {
    if *key_type != KeyType::Map {
      return Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid value data type."
      ));
    }
  }

  let mut inner = data.maps.entry(key.clone())
    .or_insert(BTreeMap::new());

  let field = match args.pop() {
    Some(RedisValue::String(s)) => s,
    Some(RedisValue::Integer(i)) => i.to_string(),
    _ => return Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Invalid field."
    ))
  };
  let field: RedisKey = field.into();

  let value = match args.pop() {
    Some(v) => v,
    None => return Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Invalid value data type."
    ))
  };

  data.key_types.insert(key.clone(), KeyType::Map);
  data.keys.insert(key.clone());

  let res = if inner.contains_key(&field) {
    0
  }else{
    1
  };

  let _ = inner.insert(Arc::new(field), value);

  Ok(Frame::Integer(res))
}

pub fn hdel(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  args.reverse();

  let key = match args.pop() {
    Some(RedisValue::String(s)) => s,
    Some(RedisValue::Integer(i)) => i.to_string(),
    _ => return utils::null()
  };
  let key = utils::get_key(&*data, key);

  if let Some(key_type) = data.key_types.get(&key) {
    if *key_type != KeyType::Map {
      return Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid value data type."
      ));
    }
  }else{
    return Ok(Frame::Integer(0))
  }

  let mut count = 0;

  if let Some(mut inner) = data.maps.get_mut(&key) {
    for field in args.into_iter() {
      let field = match field {
        RedisValue::String(s) => s,
        RedisValue::Integer(i) => i.to_string(),
        _ => return Err(RedisError::new(
          RedisErrorKind::InvalidArgument, "Invalid field."
        ))
      };
      let field: RedisKey = field.into();

      if inner.remove(&field).is_some() {
        count += 1;
      }
    }
  }

  Ok(Frame::Integer(count))
}

pub fn hexists(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  args.reverse();

  let key = match args.pop() {
    Some(RedisValue::String(s)) => s,
    Some(RedisValue::Integer(i)) => i.to_string(),
    _ => return utils::null()
  };
  let key = utils::get_key(&*data, key);

  if let Some(key_type) = data.key_types.get(&key) {
    if *key_type != KeyType::Map {
      return Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid value data type."
      ));
    }
  }else{
    return Ok(Frame::Integer(0));
  }

  if let Some(inner) = data.maps.get(&key) {
    let field = match args.pop() {
      Some(RedisValue::String(s)) => s,
      Some(RedisValue::Integer(i)) => i.to_string(),
      _ => return Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid field."
      ))
    };
    let field: RedisKey = field.into();

    let res = match inner.get(&field) {
      Some(v) => match v.kind() != RedisValueKind::Null {
        true => 1,
        false => 0
      },
      None => 0
    };

    Ok(Frame::Integer(res))
  }else{
    Ok(Frame::Integer(0))
  }
}

pub fn hgetall(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  args.reverse();

  let key = match args.pop() {
    Some(RedisValue::String(s)) => s,
    Some(RedisValue::Integer(i)) => i.to_string(),
    _ => return utils::null()
  };
  let key = utils::get_key(&*data, key);

  if let Some(key_type) = data.key_types.get(&key) {
    if *key_type != KeyType::Map {
      return Err(RedisError::new(
        RedisErrorKind::InvalidArgument, "Invalid value data type."
      ));
    }
  }else{
    return Ok(Frame::Array(vec![]))
  }

  let inner = match data.maps.get(&key) {
    Some(i) => i,
    None => return utils::null()
  };

  let mut out = Vec::with_capacity(inner.len() * 2);

  for (key, value) in inner.iter() {
    out.push(Frame::BulkString(key.key.clone().into_bytes()));
    out.push(Frame::BulkString(match value {
      &RedisValue::String(ref s) => s.to_owned().into_bytes(),
      &RedisValue::Integer(i) => i.to_string().into_bytes(),
      _ => "NULL".to_owned().into_bytes()
    }));
  }

  Ok(Frame::Array(out))
}

pub fn incr(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  args.reverse();

  let key = match args.pop() {
    Some(RedisValue::String(s)) => s,
    Some(RedisValue::Integer(i)) => i.to_string(),
    _ => return utils::null()
  };
  let key = utils::get_key(&*data, key);

  let val = match data.data.get(&key) {
    Some(&RedisValue::String(ref s)) => utils::to_int(&s)?,
    Some(&RedisValue::Integer(i)) => i.clone(),
    Some(&RedisValue::Null) | None => 0,
    _ => return Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Invalid key type."
    ))
  };

  let _ = data.data.insert(key, RedisValue::Integer(val + 1));
  Ok(Frame::Integer(val + 1))
}

pub fn decr(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  args.reverse();

  let key = match args.pop() {
    Some(RedisValue::String(s)) => s,
    Some(RedisValue::Integer(i)) => i.to_string(),
    _ => return utils::null()
  };
  let key = utils::get_key(&*data, key);

  let val = match data.data.get(&key) {
    Some(&RedisValue::String(ref s)) => utils::to_int(&s)?,
    Some(&RedisValue::Integer(i)) => i.clone(),
    Some(&RedisValue::Null) | None => 0,
    _ => return Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Invalid key type."
    ))
  };

  let _ = data.data.insert(key, RedisValue::Integer(val - 1));
  Ok(Frame::Integer(val - 1))
}

pub fn incrby(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  args.reverse();

  let key = match args.pop() {
    Some(RedisValue::String(s)) => s,
    Some(RedisValue::Integer(i)) => i.to_string(),
    _ => return Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Invalid key."
    ))
  };
  let key = utils::get_key(&*data, key);

  let count = match args.pop() {
    Some(RedisValue::String(s)) => utils::to_int(&s)?,
    Some(RedisValue::Integer(i)) => i.clone(),
    _ => return Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Invalid count."
    ))
  };

  let val = match data.data.get(&key) {
    Some(&RedisValue::String(ref s)) => utils::to_int(&s)?,
    Some(&RedisValue::Integer(i)) => i.clone(),
    Some(&RedisValue::Null) | None => 0,
    _ => return Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Invalid key type."
    ))
  };

  let _ = data.data.insert(key, RedisValue::Integer(val + count));
  Ok(Frame::Integer(val + count))
}

pub fn decrby(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  args.reverse();

  let key = match args.pop() {
    Some(RedisValue::String(s)) => s,
    Some(RedisValue::Integer(i)) => i.to_string(),
    _ => return Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Invalid key."
    ))
  };
  let key = utils::get_key(&*data, key);

  let count = match args.pop() {
    Some(RedisValue::String(s)) => utils::to_int(&s)?,
    Some(RedisValue::Integer(i)) => i.clone(),
    _ => return Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Invalid count."
    ))
  };

  let val = match data.data.get(&key) {
    Some(&RedisValue::String(ref s)) => utils::to_int(&s)?,
    Some(&RedisValue::Integer(i)) => i.clone(),
    Some(&RedisValue::Null) | None => 0,
    _ => return Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Invalid key type."
    ))
  };

  let _ = data.data.insert(key, RedisValue::Integer(val - count));
  Ok(Frame::Integer(val - count))
}

pub fn info(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  Ok(Frame::BulkString("Mock Redis Server".into()))
}

pub fn ping(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  Ok(Frame::SimpleString("PONG".into()))
}

pub fn flushall(mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  let global_data = utils::global_data_set();

  for data_ref in global_data.read().deref().values() {
    let mut data_guard = data_ref.write();
    let mut data = data_guard.deref_mut();

    data.data.clear();
    data.maps.clear();
    data.sets.clear();
    data.key_types.clear();
    data.keys.clear();
    utils::clear_expirations(&data.expirations);
  }

  utils::ok()
}

pub fn smembers(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  args.reverse();

  let key = match args.pop() {
    Some(RedisValue::String(s)) => s,
    Some(RedisValue::Integer(i)) => i.to_string(),
    _ => return Err(RedisError::new(
      RedisErrorKind::InvalidArgument, "Invalid key."
    ))
  };
  let key = utils::get_key(&*data, key);

  let members = match data.sets.get(&key) {
    Some(m) => m,
    None => return Ok(Frame::Array(vec![]))
  };

  let mut out = Vec::with_capacity(members.len());
  for member in members.iter() {
    out.push(Frame::BulkString(member.key.as_bytes().to_vec()));
  }

  Ok(Frame::Array(out))
}

pub fn publish(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  Ok(Frame::Integer(1))
}

pub fn subscribe(data: &mut DataSet, mut args: Vec<RedisValue>) -> Result<Frame, RedisError> {
  Ok(Frame::Integer(1))
}