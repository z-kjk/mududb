#[cfg(test)]
mod tests {
    #[test]
    fn sql_parser_crate_loads() {
        let parser = crate::ast::parser::SQLParser::new();
        let stmt_list = parser.parse("select id from users;").unwrap();
        assert_eq!(stmt_list.stmts().len(), 1);
    }
}
