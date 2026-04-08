use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ComponentTarget {
    #[default]
    P2,
    P3,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeOpt {
    #[serde(default)]
    pub component_target: ComponentTarget,
    pub enable_async: bool,
}

impl RuntimeOpt {
    pub fn component_target(&self) -> ComponentTarget {
        self.component_target
    }
}

impl Default for RuntimeOpt {
    fn default() -> Self {
        Self {
            component_target: ComponentTarget::P2,
            enable_async: false,
        }
    }
}
