use crate::api::time::SysTime;
use chrono::{DateTime, Utc};
use std::time::{Instant, SystemTime};

pub struct LinuxTime;

impl SysTime for LinuxTime {
    fn instant_now(&self) -> Instant {
        Instant::now()
    }

    fn system_time_now(&self) -> SystemTime {
        SystemTime::now()
    }

    fn utc_now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}
