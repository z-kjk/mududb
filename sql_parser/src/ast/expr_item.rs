use crate::ast::expr_literal::ExprLiteral;
use crate::ast::expr_name::ExprName;
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub enum ExprItem {
    ItemName(ExprName),
    ItemValue(ExprValue),
}

#[derive(Clone, Debug)]
pub enum ExprValue {
    ValueLiteral(ExprLiteral),
    ValuePlaceholder,
}

impl ExprItem {
    pub fn to_field(&self) -> Option<&ExprName> {
        if let ExprItem::ItemName(field) = self {
            Some(field)
        } else {
            None
        }
    }

    pub fn to_literal(&self) -> Option<&ExprLiteral> {
        if let ExprItem::ItemValue(ExprValue::ValueLiteral(literal)) = self {
            Some(literal)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ExprItem, ExprValue};
    use crate::ast::expr_literal::ExprLiteral;
    use crate::ast::expr_name::ExprName;
    use mudu_type::dat_type_id::DatTypeID;
    use mudu_type::dat_typed::DatTyped;

    #[test]
    fn expr_item_to_field_returns_name_only_for_name_variant() {
        let mut field = ExprName::new();
        field.set_name("id".to_string());
        let name = ExprItem::ItemName(field);
        assert_eq!(name.to_field().unwrap().name(), "id");

        let literal = ExprItem::ItemValue(ExprValue::ValueLiteral(ExprLiteral::DatumLiteral(
            DatTyped::from_i32(7),
        )));
        assert!(literal.to_field().is_none());
    }

    #[test]
    fn expr_item_to_literal_returns_literal_only_for_literal_variant() {
        let literal = ExprItem::ItemValue(ExprValue::ValueLiteral(ExprLiteral::DatumLiteral(
            DatTyped::from_string("alice".to_string()),
        )));
        assert_eq!(
            literal.to_literal().unwrap().dat_type().dat_type().dat_type_id(),
            DatTypeID::String
        );

        let placeholder = ExprItem::ItemValue(ExprValue::ValuePlaceholder);
        assert!(placeholder.to_literal().is_none());
    }
}
