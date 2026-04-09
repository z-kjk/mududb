use crate::python::py_function::PyFunction;
use crate::python::template_proc::{ArgumentInfo, ProcedureInfo, ReturnInfo, TemplateProc};
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
    pub position_fn_start: HashMap<String, (Position, bool)>,
    pub mudu_procedure: HashMap<String, Function>,
    pub position_refactor_use: Vec<UseRefactor>,
    pub lines: Vec<String>,
    pub refactor_src_dst_mod: Option<(Vec<String>, Vec<String>)>,
}


#[derive(Debug, Clone)]
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