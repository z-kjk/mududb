use std::any::Any;
use std::fmt::Debug;

pub trait ASTNode: Any + Debug + Send + Sync {}
