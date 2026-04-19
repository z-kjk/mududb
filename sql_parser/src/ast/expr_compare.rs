use std::fmt::{Debug, Formatter};

use crate::ast::ast_node::ASTNode;
use crate::ast::expr_item::{ExprItem, ExprValue};
use crate::ast::expr_literal::ExprLiteral;
use crate::ast::expr_name::ExprName;

use crate::ast::expr_operator::ValueCompare;

// currently, we only support a ExprField compare with ExprLiteral
#[derive(Clone)]
pub struct ExprCompare {
    op: ValueCompare,
    left: ExprItem,
    right: ExprItem,
}

impl ExprCompare {
    pub fn new(op: ValueCompare, left: ExprItem, right: ExprItem) -> Self {
        Self { op, left, right }
    }

    pub fn op(&self) -> &ValueCompare {
        &self.op
    }

    pub fn left(&self) -> &ExprItem {
        &self.left
    }

    pub fn right(&self) -> &ExprItem {
        &self.right
    }

    pub fn expr_field_op_literal(&self) -> Option<(ExprName, ExprLiteral, ValueCompare)> {
        match (&self.left, &self.right) {
            (ExprItem::ItemName(_l), ExprItem::ItemValue(ExprValue::ValueLiteral(_r))) => {}
            (ExprItem::ItemValue(ExprValue::ValueLiteral(l)), ExprItem::ItemName(r)) => {
                return Some((r.clone(), l.clone(), Self::revert_cmp_op(self.op)));
            }
            _ => {
                return None;
            }
        }
        None
    }

    //
    fn revert_cmp_op(op: ValueCompare) -> ValueCompare {
        ValueCompare::revert_cmp_op(op)
    }
}

impl Debug for ExprCompare {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "op: ")?;
        self.op.fmt(f)?;
        write!(f, "left: ")?;
        self.left.fmt(f)?;
        write!(f, "right: ")?;
        self.right.fmt(f)?;
        Ok(())
    }
}

impl ASTNode for ExprCompare {}

#[cfg(test)]
mod tests {
    use super::ExprCompare;
    use crate::ast::expr_item::{ExprItem, ExprValue};
    use crate::ast::expr_literal::ExprLiteral;
    use crate::ast::expr_name::ExprName;
    use crate::ast::expr_operator::ValueCompare;
    use mudu_type::dat_typed::DatTyped;

    fn field(name: &str) -> ExprItem {
        let mut expr = ExprName::new();
        expr.set_name(name.to_string());
        ExprItem::ItemName(expr)
    }

    fn literal_i32(value: i32) -> ExprItem {
        ExprItem::ItemValue(ExprValue::ValueLiteral(ExprLiteral::DatumLiteral(
            DatTyped::from_i32(value),
        )))
    }

    #[test]
    fn expr_field_op_literal_reverts_literal_field_order() {
        let cmp = ExprCompare::new(ValueCompare::GT, literal_i32(7), field("id"));
        let (field, literal, op) = cmp.expr_field_op_literal().unwrap();

        assert_eq!(field.name(), "id");
        assert_eq!(literal.dat_type().dat_internal().to_i32(), 7);
        assert!(matches!(op, ValueCompare::LE));
    }

    #[test]
    fn expr_field_op_literal_rejects_non_literal_pairs() {
        let cmp = ExprCompare::new(ValueCompare::EQ, field("lhs"), field("rhs"));
        assert!(cmp.expr_field_op_literal().is_none());
    }
}
