use crate::types::RedisTypeError;

pub(crate) fn incr_with_max(curr: u32, max: u32) -> Option<u32> {
    if max == 0 {
        Some(max)
    }else if curr >= max {
        None
    }else{
        Some(curr + 1)
    }
}

/// Convert a redis string to an `f64`, supporting "+inf" and "-inf".
pub(crate) fn redis_string_to_f64(s: &str) -> Result<f64, RedisTypeError> {
    if s == "+inf" {
        Ok(f64::INFINITY)
    }else if s == "-inf" {
        Ok(f64::NEG_INFINITY)
    }else{
        s.parse::<f64>().map_err(|_| RedisTypeError(format!("Could not convert {} to floating point value.", s)))
    }
}