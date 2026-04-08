use crate::api::env::SysEnv;
use crate::linux::env::LinuxSysEnv;
use std::sync::{Arc, OnceLock, RwLock};

static DEFAULT_ENV: OnceLock<RwLock<Arc<dyn SysEnv>>> = OnceLock::new();

fn default_env_cell() -> &'static RwLock<Arc<dyn SysEnv>> {
    DEFAULT_ENV.get_or_init(|| RwLock::new(Arc::new(LinuxSysEnv::new())))
}

pub fn default_env() -> Arc<dyn SysEnv> {
    default_env_cell()
        .read()
        .expect("default sys env lock poisoned")
        .clone()
}

pub fn set_default_env(env: Arc<dyn SysEnv>) {
    let mut guard = default_env_cell()
        .write()
        .expect("default sys env lock poisoned");
    *guard = env;
}

pub fn reset_default_env() {
    set_default_env(Arc::new(LinuxSysEnv::new()));
}
