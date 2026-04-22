#[cfg(test)]
mod test {
    use crate::debug::async_debug_serve;
    use crate::log::log_setup;
    use crate::notifier::NotifyWait;
    use crate::task::spawn_local_task_timeout;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::runtime::Runtime;
    use tokio::task::LocalSet;

    #[test]
    fn test_server() {
        log_setup("debug");
        let addr: SocketAddr = ([127, 0, 0, 1], 3000).into();
        let runtime = Runtime::new().unwrap();
        let local = LocalSet::new();
        local.spawn_local(async move {
            spawn_local_task_timeout(NotifyWait::new(), Duration::from_secs(1), "", async move {
                async_debug_serve(addr).await
            })
        });
        let _ = runtime.block_on(local);
    }
}
