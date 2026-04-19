use crate::env::default_env;
use uuid::Uuid;

pub trait SysRandom: Send + Sync {
    fn uuid_v4(&self) -> Uuid;
}

pub fn uuid_v4() -> Uuid {
    default_env().random().uuid_v4()
}

pub fn next_uuid_v4_string() -> String {
    uuid_v4().to_string()
}
