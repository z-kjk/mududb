//! python.ver
//! 4.27 添加了import的定义以及构建方法
use crate::python::function::PyFunction;
use crate::python::tymplate_proc::{ArgumentInfo, ProcedureInfo, ReturnInfo, TemplateProc};
use askama::Template;
use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;
use mudu::utils::case_convert::{to_kebab_case, to_pascal_case};
use mudu_binding::universal::uni_type_desc::UniTypeDesc;
use mudu_contract::procedure::proc;
use mudu_contract::procedure::proc_desc::ProcDesc;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use tree_sitter::Node;

#[derive(Debug, Clone)]
pub struct UseRefactor {
    pub start_position: Position,
    pub end_position: Position,
    pub src_string: String,
    pub dst_string: String,
}

#[derive(Debug)]
pub struct ParseContext {
    pub text: String,
    pub sys_call: HashSet<String>,
    /// callee key -> caller value
    pub call_dependencies: HashMap<String, HashSet<String>>,
    pub position_call_end: HashMap<String, Vec<(Position, bool)>>,
    pub position_def_start: HashMap<String, (Position, bool)>,
    pub mudu_procedure: HashMap<String, PyFunction>,
    pub position_refactor_use: Vec<UseRefactor>, //import版本
    pub lines: Vec<String>,
    pub refactor_src_dst_mod: Option<(Vec<String>, Vec<String>)>,
}


impl ParseContext {

    ///加了重构版
    pub fn new(text: String, src_mod: Option<String>, dst_mod: Option<String>) -> Self {
        let mut sys_call = HashSet::new();
        sys_call.insert("mudu_query".to_string());
        sys_call.insert("mudu_command".to_string());
        sys_call.insert("mudu_open".to_string());
        sys_call.insert("mudu_close".to_string());
        sys_call.insert("mudu_get".to_string());
        sys_call.insert("mudu_put".to_string());
        sys_call.insert("mudu_range".to_string());

        let lines: Vec<String> = text.lines().map(|s| s.to_string()).collect();

        let refactor_src_dst_mod = if let Some(src) = src_mod
            && let Some(dst) = dst_mod
        {
            let src_path = mod_path_to_vec(&src);
            let dst_path = mod_path_to_vec(&dst);
            if src_path == dst_path || src_path.len() != dst_path.len() {
                None
            } else {
                Some((src_path, dst_path))
            }
        } else {
            None
        };

        Self {
            text,
            sys_call,
            call_dependencies: Default::default(),
            position_call_end: Default::default(),
            position_def_start: Default::default(),
            mudu_procedure: Default::default(),
            position_refactor_use: Default::default(),
            lines,
            refactor_src_dst_mod,
        }
    }


    pub fn node_text(&self, node: &Node) -> RS<String> {
        let s = node
            .utf8_text(self.text.as_bytes())
            .map_err(|e| m_error!(EC::DecodeErr, "decode utf8 error", e))?;
        Ok(s.to_string())
    }

    pub fn is_sys_call(&self, name: &str) -> bool {
        self.sys_call.contains(name)
    }

    pub fn add_func_call_end_position(
        &mut self,
        fn_name: String,
        end_position: Position,
        sys_call: bool,
    ) {
        let opt = self.position_call_end.get_mut(&fn_name);
        if let Some(vec) = opt {
            vec.push((end_position, sys_call));
        } else {
            self.position_call_end
                .insert(fn_name, vec![(end_position, sys_call)]);
        }
    }

    pub fn add_call_dependency(&mut self, caller: &str, callee: &str) {
        if let Some(set) = self.call_dependencies.get_mut(callee) {
            set.insert(caller.to_string());
        } else {
            // 用 caller.to_string() 构建初始的 Vec，这样生成的就是 HashSet<String>
            let caller_set = HashSet::from_iter(vec![caller.to_string()]);

            // 或者更现代/简洁的写法是直接使用 HashSet::from:
            // let caller_set = HashSet::from([caller.to_string()]);

            self.call_dependencies.insert(callee.to_string(), caller_set);
        }
    }


}

fn mod_path_to_vec(s: &str) -> Vec<String> {
    s.split('.').map(|x| x.to_string()).collect()
}

#[derive(Debug, Clone, PartialEq)]
pub struct Position {
    pub row: usize,
    pub col: usize,
}

impl Position {
    pub fn from_ts(pos: tree_sitter::Point) -> Self {
        Self {
            row: pos.row,
            col: pos.column,
        }
    }
}