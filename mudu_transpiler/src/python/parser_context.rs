//! python.ver
//! 4.27 添加了import的定义以及构建方法
//! 4.30 增加了tran_to_async函数
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
    pub refactor_src_dst_mod: Option<(Vec<String>, Vec<String>)>, //用来干啥的？
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

    ///v2添加
    pub fn tran_to_async(&mut self) {
        // 种子：sys_call 是已知的异步函数（如 mudu_open 等）
        let mut callees: HashSet<String> = self.sys_call.clone();
        let mut callers: HashSet<String> = HashSet::default();

        // 暂时移出避免借用冲突
        let mut position_def_start = std::mem::take(&mut self.position_def_start);
        let mut position_call_end = std::mem::take(&mut self.position_call_end);

        // 不动点循环
        while !callees.is_empty() || !callers.is_empty() {
            self.update_async_await_walk_dependency(
                &mut callers,
                &mut callees,
                &mut position_def_start,
                &mut position_call_end,
            );
        }

        // 写回
        self.position_def_start = position_def_start;
        self.position_call_end = position_call_end;
    }

    fn get_caller_of_callee(&self, callee: &String) -> Option<&HashSet<String>> {
        self.call_dependencies.get(callee)
    }

    pub fn update_async_await_walk_dependency(
        &self,
        callers: &mut HashSet<String>,
        callees: &mut HashSet<String>,
        position_def_start: &mut HashMap<String, (Position, bool)>,
        position_call_end: &mut HashMap<String, Vec<(Position, bool)>>,
    ) {
        for callee in callees.iter() {
            self.mark_all_async_caller(callee, callers, position_def_start);
        }
        callees.clear();

        for caller in callers.iter() {
            self.mark_all_async_callee(caller, callees, position_call_end);
        }
        callers.clear();
    }

    pub fn mark_all_async_caller(
        &self,
        callee: &String,
        callers: &mut HashSet<String>,
        position_def_start: &mut HashMap<String, (Position, bool)>,
    ) {
        let _set = HashSet::default();
        // ↓ 唯一变化：字段名从 position_fn_start 改为 position_def_start
        let set = self.call_dependencies.get(callee).unwrap_or(&_set);
        for caller in set {
            if let Some((_pos, is_async)) = position_def_start.get_mut(caller) {
                if !*is_async {
                    *is_async = true;
                    callers.insert(caller.clone());
                }
            }
            // 递归 DFS 向上继续扩散
            self.mark_all_async_caller(caller, callers, position_def_start);
        }
    }

    pub fn mark_all_async_callee(
        &self,
        caller: &String,
        callees: &mut HashSet<String>,
        position_call_end: &mut HashMap<String, Vec<(Position, bool)>>,
    ) {
        let mut _vec = Vec::new();
        let vec = position_call_end.get_mut(caller).unwrap_or(&mut _vec);
        for (_, is_async) in vec.iter_mut() {
            if !*is_async {
                *is_async = true;
                callees.insert(caller.clone());
            }
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