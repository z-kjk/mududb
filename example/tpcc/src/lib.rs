#[allow(unused)]
#[cfg(target_arch = "x86_64")]
pub mod rust;

#[allow(unused)]
#[cfg(target_arch = "wasm32")]
pub mod generated;

#[cfg(test)]
pub(crate) fn test_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}
