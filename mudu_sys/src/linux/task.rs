use crate::api::task::SysTask;
use async_trait::async_trait;
use mudu::common::result::RS;
use std::time::Duration;

pub struct LinuxTask;

#[async_trait]
impl SysTask for LinuxTask {
    async fn sleep(&self, dur: Duration) -> RS<()> {
        tokio::time::sleep(dur).await;
        Ok(())
    }

    fn sleep_blocking(&self, dur: Duration) {
        std::thread::sleep(dur);
    }
}
