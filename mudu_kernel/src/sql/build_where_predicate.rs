use crate::contract::table_desc::TableDesc;
use mudu::common::buf::Buf as Datum;
use mudu::common::id::OID;
use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;
use sql_parser::ast::expr_compare::ExprCompare;
use sql_parser::ast::expr_literal::ExprLiteral;
use sql_parser::ast::expr_name::ExprName;
use sql_parser::ast::expr_operator::ValueCompare;

fn convert_expr_compare_equal(
    _expr: &ExprName,
    _expr_literal: &ExprLiteral,
    _desc: &TableDesc,
) -> RS<(OID, Datum)> {
    Err(m_error!(
        EC::NotImplemented,
        "equality predicate conversion is not implemented"
    ))
}

fn convert_expr_compare(expr: &ExprCompare, _desc: &TableDesc) -> RS<(OID, Datum)> {
    match expr.op() {
        ValueCompare::EQ => match (expr.left(), expr.right()) {
            _ => Err(m_error!(
                EC::NotImplemented,
                "only simple equality predicates are supported"
            )),
        },
        _ => Err(m_error!(
            EC::NotImplemented,
            "non-equality predicates are not implemented"
        )),
    }
}

pub fn convert_exprs(exprs: &Vec<ExprCompare>, table_desc: &TableDesc) -> RS<Vec<(OID, Datum)>> {
    let mut vec = vec![];
    for expr in exprs.iter() {
        let datum = convert_expr_compare(expr, table_desc)?;
        vec.push(datum)
    }
    Ok(vec)
}
