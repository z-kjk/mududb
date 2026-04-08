use crate::api::random::SysRandom;
use uuid::Uuid;

pub struct LinuxRandom;

impl SysRandom for LinuxRandom {
    fn uuid_v4(&self) -> Uuid {
        Uuid::new_v4()
    }
}
