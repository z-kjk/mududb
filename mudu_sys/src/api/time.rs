use crate::env::default_env;
use chrono::{DateTime, Utc};
use std::time::{Instant, SystemTime};

pub trait SysTime: Send + Sync {
    fn instant_now(&self) -> Instant;
    fn system_time_now(&self) -> SystemTime;
    fn utc_now(&self) -> DateTime<Utc>;
}

pub fn instant_now() -> Instant {
    default_env().time().instant_now()
}

pub fn system_time_now() -> SystemTime {
    default_env().time().system_time_now()
}

pub fn utc_now() -> DateTime<Utc> {
    default_env().time().utc_now()
}
