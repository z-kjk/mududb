use crate::env::default_env;
use crate::fd::RawFd;
use mudu::common::result::RS;

pub fn eventfd() -> RS<RawFd> {
    default_env().sync().eventfd()
}

pub fn notify_eventfd(fd: RawFd) -> RS<()> {
    default_env().sync().notify_eventfd(fd)
}

pub fn read_eventfd(fd: RawFd) -> RS<u64> {
    default_env().sync().read_eventfd(fd)
}

pub fn close_fd(fd: RawFd) -> RS<()> {
    default_env().sync().close_fd(fd)
}
