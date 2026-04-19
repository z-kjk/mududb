//! python_parser.rs
//! 第一版最小可运行骨架：
//! - 提取 function_definition
//! - 提取参数 typed_parameter
//! - 提取 call
//! - 提取 attribute 调用名

use crate::python::parser_context::{ParseContext, Position, UseRefactor};
use crate::python::ts_const;
use mudu::common::result::RS; //error返回，好像还行
use mudu::error::ec::EC;
use mudu::m_error;
use std::collections::HashMap;
use crate::python::function::PyFunction;
use crate::python::python_type::PythonType;
use tree_sitter::{Language, Node, Parser};
use tree_sitter_python;

const MUDU_PROC_MARKER: &str = "#mudu_proc#";

pub struct PythonParser {}

fn rust_language() -> Language {
    tree_sitter_python::LANGUAGE.into()
}


impl PythonParser {
    fn new() -> PythonParser {
        Self {}
    }

    pub fn parse(context: &mut ParseContext) -> RS<()> {
        let mut parser = Parser::new();
        parser.set_language(&rust_language()).unwrap();
        let tree = parser.parse(&context.text, None).unwrap();
        let node = tree.root_node();
        let parser = Self::new();
        parser.walk_node(context, node, None)?;
        Ok(())
    }

    fn walk_node(
        &self,
        context: &mut ParseContext,
        node: Node,
        opt_function_name: Option<&str>,
    ) -> RS<()> {
        let mut cursor = node.walk();
        for (_, child) in node.children(&mut cursor).enumerate() {
            let kind = child.kind();
            match kind {
                //todo 缺个import(应该）
                ts_const::ts_kind_name::S_FUNCTION_DEFINITION  => {
                    self.visit_function_definition(context, child)?;
                }
                ts_const::ts_kind_name::S_CALL   => {
                    self.visit_call(context, child, opt_function_name)?;
                }
                _ => {
                    self.walk_node(context, child, opt_function_name)?;
                }
            }
        }
        Ok(())
    }

    //todo 不知道是否要看是否适配
    fn is_mudu_proc(&self, context: &ParseContext, function_item_start_row: usize) -> bool {
        if context.lines.len() < function_item_start_row {
            panic!("row index out of bounds");
        }
        for i in 1..function_item_start_row {
            let line = &context.lines[function_item_start_row - i];
            let line_trim = line.trim();
            if line_trim == MUDU_PROC_MARKER {
                return true;
            } else if !line_trim.is_empty() {
                return false;
            }
        }
        false
    }

    ///todo 判断是否有类型注解，在挨个节点提取的时候写吧
    fn validate_python_function_type_hints<'tree>(
        node: &Node<'tree>,
        context: &ParseContext //为什么
    ) -> RS<()>{
        Ok(())
    }
    //函数定义
    ///负责
    /// 从function中提取name,parameters(parser_parameters),return_type,body
    fn visit_function_definition(&self, context: &mut ParseContext, node: Node) -> RS<()> {
        let mut cursor = node.walk(); //遍历游标，遍历node的子节点
        let mut contains_async = false; //是否包含async修饰
        let mut def_start_pos = None; //原本报错，然后因为python是def，所以进行修改。
        let start_pos = node.start_position();
        let is_mudu_proc = self.is_mudu_proc(context, start_pos.row);
        for child in node.children(&mut cursor) {
            match child.kind() {
                ts_const::ts_kind_name::S_DEF => {
                // "def" => {
                    let pos = child.start_position();
                    def_start_pos = Some(Position::from_ts(pos));
                }
                ts_const::ts_kind_name::S_ASYNC => {
                // "async" => {
                    contains_async = true; //直接打印，确定是否异步
                }
                _ => {}
            }
        }
        let name_node = crate::python::python_parser::expected_child_field(&node, ts_const::ts_field_name::NAME)?;
        let body_node = crate::python::python_parser::expected_child_field(&node, ts_const::ts_field_name::BODY)?;
        //todo 好像得带上类型注解判断
        let function_name = context.node_text(&name_node)?;
        if let Some(pos) = def_start_pos {
            context
                .position_def_start
                .insert(function_name.clone(), (pos, contains_async));
        }
        if is_mudu_proc {
            let parameters = expected_child_field(&node, ts_const::ts_field_name::PARAMETERS)?;
            let vec_parameters = self.visit_parameters(context, parameters)?;
            let opt_return_type = crate::python::python_parser::opt_child_field(&node, ts_const::ts_field_name::RETURN_TYPE);
            let opt_ret_python_type = match opt_return_type {
                Some(return_type) => Some(self.visit_type(context, return_type)?),
                _ => None,
            };
            let function = crate::python::function::PyFunction {
                name: function_name.clone(),
                arg_list: vec_parameters,
                return_type: opt_ret_python_type,
                is_async: false,
            };
            context
                .mudu_procedure
                .insert(function.name.clone(), function);
        }
        self.walk_node(context, body_node, Some(function_name.as_str()))?; //引用

        Ok(())
    }

    //todo 函数调用
    ///负责，从call里取
    /// function、arguments
    fn visit_call(&self, context: &mut ParseContext, node: Node, opt_function_name: Option<&str>) -> RS<()>{
        Ok(())
    }

    ///遍历函数参数列表
    fn visit_parameters(
        &self,
        parse_context: &ParseContext,
        node: Node,
    ) -> RS<Vec<(String, crate::python::python_type::PythonType)>> {
        let mut cursor = node.walk();
        let mut vec = Vec::new();
        for child in node.children(&mut cursor) {
            if child.kind() == ts_const::ts_kind_name::S_TYPED_PARAMETER { //找到带参数类型注解的节点
                let (name, ty) = self.visit_parameter(parse_context, child)?;
                vec.push((name, ty));
            }
        }
        Ok(vec)
    }

    ///解析单参数的名字和类型
    fn visit_parameter(&self, context: &ParseContext, node: Node) -> RS<(String, crate::python::python_type::PythonType)> {
        let type_node = expected_child_field(&node, ts_const::ts_field_name::TYPE)?; //参数类型

        let mut cursor = node.walk();
        let name_node = node
            .children(&mut cursor)
            .find(|child| child.kind() == ts_const::ts_kind_name::S_IDENTIFIER) //第一个identifier来找参数名
            .ok_or_else(|| m_error!(EC::NoneErr, "cannot find parameter name"))?;

        let py_type = self.visit_type(context, type_node)?;
        let argument_name = context.node_text(&name_node)?;
        Ok((argument_name, py_type))
    }

    ///解析类型节点入口
    fn visit_type(&self, context: &ParseContext, node: Node) -> RS<PythonType> {
        match node.kind() {
            ts_const::ts_kind_name::S_TYPE => { //外壳，继续递归
                let mut cursor = node.walk();
                let inner = node
                    .children(&mut cursor)
                    .find(|child| child.is_named())
                    .ok_or_else(|| m_error!(EC::NoneErr, "type node has no inner named child"))?;
                self.visit_type(context, inner)
            }

            ts_const::ts_kind_name::S_IDENTIFIER => { //基本类型
                let type_name = context.node_text(&node)?;
                Ok(self.ident_to_python_type(type_name))
            }

            ts_const::ts_kind_name::S_GENERIC_TYPE => { //元组类型
                let (name, args) = self.visit_generic_type(context, node)?; //由泛型函数进行处理
                match name.as_str() {
                    "tuple" | "Tuple" => Ok(PythonType::Tuple(args)),
                    "Union" | "union" => Ok(PythonType::Union(args)),
                    _ => Ok(PythonType::Generic(name, args)),
                }
            }

            _ => {
                let kind = node.kind();
                let text = context.node_text(&node)?;
                Err(m_error!(
                EC::NoneErr,
                format!("node kind {}, do not support type {}", kind, text)
            ))
            }

        }

    }

    fn ident_to_python_type(&self, name: String) -> PythonType {
        match name.as_str() {
            "int" | "float" | "bool" | "str" | "bytes" => PythonType::Primitive(name),
            "Any" => PythonType::Any,
            "None" => PythonType::NoneType,
            _ => PythonType::Custom(name),
        }
    }
    
    ///解析泛型类型
    fn visit_generic_type(
        &self,
        context: &ParseContext,
        node: Node,
    ) -> RS<(String, Vec<PythonType>)> {
        let mut cursor = node.walk();

        let ident_node = node
            .children(&mut cursor)
            .find(|child| child.kind() == ts_const::ts_kind_name::S_IDENTIFIER) //找泛型名
            .ok_or_else(|| m_error!(EC::NoneErr, "generic_type missing identifier"))?;

        let name = context.node_text(&ident_node)?;

        let mut cursor = node.walk();
        let type_param_node = node
            .children(&mut cursor)
            .find(|child| child.kind() == ts_const::ts_kind_name::S_TYPE_PARAMETER) //参数区
            .ok_or_else(|| m_error!(EC::NoneErr, "generic_type missing type_parameter"))?;

        let args = self.visit_type_parameter(context, type_param_node)?; //递归处理参数
        Ok((name, args))
    }

    ///解析泛型参数列表
    fn visit_type_parameter(
        &self,
        context: &ParseContext,
        node: Node,
    ) -> RS<Vec<PythonType>> {
        let mut cursor = node.walk();
        let mut vec = Vec::new();

        for child in node.children(&mut cursor) {
            if child.kind() == ts_const::ts_kind_name::S_TYPE { //根据type进行解析
                let ty = self.visit_type(context, child)?;
                vec.push(ty);
            }
        }

        Ok(vec)
    }

    ///解析返回类型
    //todo 待补充吧
    fn visit_return_type(&self, context: &ParseContext, node: Node) -> RS<PythonType> {
        self.visit_type(context, node)
    }


}
//期待field存在
fn expected_child_field<'tree>(node: &Node<'tree>, field: &str) -> RS<Node<'tree>> {
    let child = node.child_by_field_name(field).map_or_else(
        || {
            Err(m_error!(
                EC::NoneErr,
                format!("cannot find child filed for {}", field)
            ))
        },
        |child| Ok(child),
    )?;
    Ok(child)
}
//field可能存在，也可能没有，没有就返回None
fn opt_child_field<'tree>(node: &Node<'tree>, field: &str) -> Option<Node<'tree>> {
    node.child_by_field_name(field)
}
