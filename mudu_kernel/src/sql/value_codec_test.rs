#[cfg(test)]
mod tests {
    use crate::sql::value_codec::ValueCodec;
    use mudu_type::dat_type::DatType;
    use mudu_type::dat_type_id::DatTypeID;
    use mudu_type::dat_typed::DatTyped;
    use mudu_type::datum::DatumDyn;
    use sql_parser::ast::expr_item::ExprValue;
    use sql_parser::ast::expr_literal::ExprLiteral;

    #[test]
    fn placeholder_consumes_parameters_in_order() {
        let mut param_index = 0;
        let first = ValueCodec::binary_from_expr(
            &ExprValue::ValuePlaceholder,
            &DatType::default_for(DatTypeID::I32),
            &(7i32, 9i32),
            &mut param_index,
        )
        .unwrap();
        let second = ValueCodec::binary_from_expr(
            &ExprValue::ValuePlaceholder,
            &DatType::default_for(DatTypeID::I32),
            &(7i32, 9i32),
            &mut param_index,
        )
        .unwrap();

        assert_eq!(param_index, 2);
        assert_eq!(
            first.as_slice(),
            7i32.to_binary(&DatType::default_for(DatTypeID::I32))
                .unwrap()
                .as_ref()
        );
        assert_eq!(
            second.as_slice(),
            9i32.to_binary(&DatType::default_for(DatTypeID::I32))
                .unwrap()
                .as_ref()
        );
    }

    #[test]
    fn placeholder_errors_when_parameter_is_missing() {
        let mut param_index = 0;
        let err = ValueCodec::binary_from_expr(
            &ExprValue::ValuePlaceholder,
            &DatType::default_for(DatTypeID::I32),
            &(),
            &mut param_index,
        )
        .unwrap_err();

        assert!(err.to_string().contains("missing parameter 0"));
    }

    #[test]
    fn literal_is_encoded_via_literal_path() {
        let mut param_index = 0;
        let binary = ValueCodec::binary_from_expr(
            &ExprValue::ValueLiteral(ExprLiteral::DatumLiteral(DatTyped::from_i32(42))),
            &DatType::default_for(DatTypeID::I32),
            &(),
            &mut param_index,
        )
        .unwrap();

        assert_eq!(param_index, 0);
        assert_eq!(
            binary.as_slice(),
            42i32
                .to_binary(&DatType::default_for(DatTypeID::I32))
                .unwrap()
                .as_ref()
        );
    }

    #[test]
    fn i64_literal_is_narrowed_for_i32_columns() {
        let mut param_index = 0;
        let binary = ValueCodec::binary_from_expr(
            &ExprValue::ValueLiteral(ExprLiteral::DatumLiteral(DatTyped::from_i64(42))),
            &DatType::default_for(DatTypeID::I32),
            &(),
            &mut param_index,
        )
        .unwrap();

        assert_eq!(param_index, 0);
        assert_eq!(
            binary.as_slice(),
            42i32
                .to_binary(&DatType::default_for(DatTypeID::I32))
                .unwrap()
                .as_ref()
        );
    }
}
