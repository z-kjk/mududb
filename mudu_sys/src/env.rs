use crate::api::env::SysEnv;
#[cfg(target_os = "linux")]
use crate::linux::env::LinuxSysEnv;
#[cfg(not(target_os = "linux"))]
use crate::portable::env::PortableSysEnv;
use std::sync::{Arc, OnceLock, RwLock};

static DEFAULT_ENV: OnceLock<RwLock<Arc<dyn SysEnv>>> = OnceLock::new();

fn default_env_cell() -> &'static RwLock<Arc<dyn SysEnv>> {
    DEFAULT_ENV.get_or_init(|| RwLock::new(Arc::new(default_sys_env())))
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
    set_default_env(Arc::new(default_sys_env()));
}

#[cfg(target_os = "linux")]
fn default_sys_env() -> impl SysEnv {
    LinuxSysEnv::new()
}

#[cfg(not(target_os = "linux"))]
fn default_sys_env() -> impl SysEnv {
    PortableSysEnv::new()
}
