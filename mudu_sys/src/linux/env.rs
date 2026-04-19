use crate::api::env::SysEnv;
use crate::api::fs::SysFs;
use crate::api::net::SysNet;
use crate::api::random::SysRandom;
use crate::api::sync::SysSync;
use crate::api::task::SysTask;
use crate::api::time::SysTime;
use crate::linux::fs::LinuxFs;
use crate::linux::net::LinuxNet;
use crate::linux::random::LinuxRandom;
use crate::linux::sync::LinuxSync;
use crate::linux::task::LinuxTask;
use crate::linux::time::LinuxTime;

pub struct LinuxSysEnv {
    time: LinuxTime,
    random: LinuxRandom,
    fs: LinuxFs,
    net: LinuxNet,
    task: LinuxTask,
    sync: LinuxSync,
}

impl LinuxSysEnv {
    pub fn new() -> Self {
        Self {
            time: LinuxTime,
            random: LinuxRandom,
            fs: LinuxFs,
            net: LinuxNet,
            task: LinuxTask,
            sync: LinuxSync,
        }
    }
}

impl SysEnv for LinuxSysEnv {
    fn time(&self) -> &dyn SysTime {
        &self.time
    }

    fn random(&self) -> &dyn SysRandom {
        &self.random
    }

    fn fs(&self) -> &dyn SysFs {
        &self.fs
    }

    fn net(&self) -> &dyn SysNet {
        &self.net
    }

    fn task(&self) -> &dyn SysTask {
        &self.task
    }

    fn sync(&self) -> &dyn SysSync {
        &self.sync
    }
}
