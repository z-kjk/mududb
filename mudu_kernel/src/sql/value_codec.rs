use mudu::common::buf::Buf;
use mudu::common::result::RS;
use mudu::error::ec::EC as ER;
use mudu::m_error;
use mudu_contract::database::sql_params::SQLParams;
use mudu_type::datum::DatumDyn;
use mudu_type::dt_fn_param::DatType;
use sql_parser::ast::expr_item::ExprValue;
use sql_parser::ast::expr_literal::ExprLiteral;

pub(crate) struct ValueCodec;

impl ValueCodec {
    pub(crate) fn binary_from_expr(
        expr: &ExprValue,
        dat_type: &DatType,
        params: &dyn SQLParams,
        param_index: &mut usize,
    ) -> RS<Buf> {
        match expr {
            ExprValue::ValueLiteral(literal) => Self::binary_from_literal(literal, dat_type),
            ExprValue::ValuePlaceholder => {
                let index = *param_index as u64;
                let datum = params.get_idx(index).ok_or_else(|| {
                    m_error!(ER::IndexOutOfRange, format!("missing parameter {}", index))
                })?;
                *param_index += 1;
                datum.to_binary(dat_type).map(|binary| binary.into())
            }
        }
    }

    pub(crate) fn binary_from_literal(literal: &ExprLiteral, dat_type: &DatType) -> RS<Buf> {
        match literal {
            ExprLiteral::DatumLiteral(typed) => typed
                .dat_internal()
                .to_binary(dat_type)
                .map(|binary| binary.into())
                .map_err(|e| m_error!(ER::TypeBaseErr, "literal type mismatch", e)),
        }
    }
}
