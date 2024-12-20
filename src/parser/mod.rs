// src/parser.rs

mod lexer;
mod ast;

use lexer::{Lexer, Token};
use ast::{*, Value};
use crate::error::Error;

pub struct Parser<'a> {
    lexer: Lexer<'a>,
    current_token: Token,
    peek_token: Token,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Result<Self, Error> {
        let mut lexer = Lexer::new(input);
        let current_token = lexer.next_token()?;
        let peek_token = lexer.next_token()?;
        
        Ok(Parser {
            lexer,
            current_token,
            peek_token,
        })
    }

    fn next_token(&mut self) -> Result<(), Error> {
        self.current_token = std::mem::replace(&mut self.peek_token, self.lexer.next_token()?);
        Ok(())
    }

    fn expect_token(&mut self, expected: Token) -> Result<(), Error> {
        if self.current_token == expected {
            self.next_token()?;
            Ok(())
        } else {
            Err(Error::Syntax(format!(
                "Expected token {:?}, got {:?}",
                expected, self.current_token
            )))
        }
    }

    pub fn parse_statement(&mut self) -> Result<Statement, Error> {
        match &self.current_token {
            Token::Select => self.parse_select(),
            Token::Insert => self.parse_insert(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            Token::Create => self.parse_create(),
            Token::Drop   => self.parse_drop(),
            Token::Alter  => self.parse_alter(),
            _ => Err(Error::Syntax(format!(
                "Unexpected token {:?} at start of statement",
                self.current_token
            ))),
        }
    }

    fn parse_select(&mut self) -> Result<Statement, Error> {
        self.next_token()?; // consume SELECT
        
        let distinct = if matches!(self.current_token, Token::Distinct) {
            self.next_token()?;
            true
        } else {
            false
        };

        let columns = self.parse_select_columns()?;

        self.expect_token(Token::From)?;
        let from = self.parse_table_reference()?;

        let joins = self.parse_joins()?;
        let where_clause = self.parse_where_clause()?;
        let group_by = self.parse_group_by()?;
        let having = self.parse_having()?;
        let order_by = self.parse_order_by()?;
        let limit = self.parse_limit()?;

        Ok(Statement::Select(SelectStatement {
            distinct,
            columns,
            from,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
        }))
    }

    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>, Error> {
        let mut columns = Vec::new();
        
        loop {
            let expr = self.parse_expr(0)?;
            let alias = if matches!(self.peek_token, Token::As) {
                self.next_token()?; // consume AS
                self.next_token()?; // move to alias
                match &self.current_token {
                    Token::Identifier(name) => {
                        self.next_token()?;
                        Some(name.clone())
                    }
                    _ => return Err(Error::Syntax("Expected identifier after AS".to_string())),
                }
            } else {
                None
            };
            
            columns.push(SelectColumn { expr, alias });
            
            match self.current_token {
                Token::Comma => {
                    self.next_token()?;
                }
                _ => break,
            }
        }
        
        Ok(columns)
    }

    fn parse_table_reference(&mut self) -> Result<TableReference, Error> {
        let schema = if matches!(self.peek_token, Token::Period) {
            let schema = match &self.current_token {
                Token::Identifier(name) => Some(name.clone()),
                _ => return Err(Error::Syntax("Expected schema name".to_string())),
            };
            self.next_token()?; // consume schema
            self.next_token()?; // consume .
            schema
        } else {
            None
        };

        let name = match &self.current_token {
            Token::Identifier(name) => name.clone(),
            _ => return Err(Error::Syntax("Expected table name".to_string())),
        };
        self.next_token()?;

        let alias = if matches!(self.current_token, Token::As) || 
                      matches!(self.current_token, Token::Identifier(_)) {
            if matches!(self.current_token, Token::As) {
                self.next_token()?;
            }
            match &self.current_token {
                Token::Identifier(alias) => {
                    self.next_token()?;
                    Some(alias.clone())
                }
                _ => return Err(Error::Syntax("Expected alias after AS".to_string())),
            }
        } else {
            None
        };

        Ok(TableReference {
            schema,
            name,
            alias,
        })
    }

    fn parse_joins(&mut self) -> Result<Vec<JoinClause>, Error> {
        let mut joins = Vec::new();
        
        while matches!(self.current_token,
            Token::Join | Token::LeftJoin | Token::RightJoin | Token::FullJoin | Token::CrossJoin)
        {
            let join_type = match self.current_token {
                Token::Join => JoinType::Inner,
                Token::LeftJoin => JoinType::Left,
                Token::RightJoin => JoinType::Right,
                Token::FullJoin => JoinType::Full,
                Token::CrossJoin => JoinType::Cross,
                _ => unreachable!(),
            };
            self.next_token()?;

            let table = self.parse_table_reference()?;

            let (on, using) = if matches!(self.current_token, Token::On) {
                self.next_token()?;
                (Some(self.parse_expr(0)?), None)
            } else if matches!(self.current_token, Token::Using) {
                self.next_token()?;
                self.expect_token(Token::LeftParen)?;
                let columns = self.parse_identifier_list()?;
                self.expect_token(Token::RightParen)?;
                (None, Some(columns))
            } else {
                (None, None)
            };

            joins.push(JoinClause {
                join_type,
                table,
                on,
                using,
            });
        }
        
        Ok(joins)
    }

    fn parse_where_clause(&mut self) -> Result<Option<Expr>, Error> {
        if matches!(self.current_token, Token::Where) {
            self.next_token()?;
            Ok(Some(self.parse_expr(0)?))
        } else {
            Ok(None)
        }
    }

    fn parse_expr(&mut self, precedence: u8) -> Result<Expr, Error> {
        let mut left = self.parse_prefix_expr()?;

        while !matches!(self.current_token, Token::EOF | Token::Semicolon)
            && precedence < self.get_precedence(&self.current_token)
        {
            left = self.parse_infix_expr(left)?;
        }

        Ok(left)
    }

    fn get_precedence(&self, token: &Token) -> u8 {
        match token {
            Token::Or => 1,
            Token::And => 2,
            Token::Equals | Token::NotEquals => 3,
            Token::Less | Token::Greater | Token::LessEqual | Token::GreaterEqual => 4,
            Token::Plus | Token::Minus => 5,
            Token::Multiply | Token::Divide | Token::Modulo => 6,
            _ => 0,
        }
    }

    fn parse_prefix_expr(&mut self) -> Result<Expr, Error> {
        match &self.current_token {
            Token::Identifier(name) => {
                self.next_token()?;
                Ok(Expr::Column(ColumnRef {
                    name: name.clone(),
                    table: None,
                    schema: None,
                }))
            }
            Token::Number(n) => {
                self.next_token()?;
                Ok(Expr::Literal(Value::Number(n.clone())))
            }
            Token::String(s) => {
                self.next_token()?;
                Ok(Expr::Literal(Value::String(s.clone())))
            }
            Token::True => {
                self.next_token()?;
                Ok(Expr::Literal(Value::Boolean(true)))
            }
            Token::False => {
                self.next_token()?;
                Ok(Expr::Literal(Value::Boolean(false)))
            }
            Token::Null => {
                self.next_token()?;
                Ok(Expr::Literal(Value::Null))
            }
            Token::LeftParen => {
                self.next_token()?;
                let expr = self.parse_expr(0)?;
                self.expect_token(Token::RightParen)?;
                Ok(expr)
            }
            Token::Not => {
                self.next_token()?;
                let expr = self.parse_expr(7)?;
                Ok(Expr::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(expr),
                })
            }
            Token::Minus => {
                self.next_token()?;
                let expr = self.parse_expr(7)?;
                Ok(Expr::Unary {
                    op: UnaryOp::Negative,
                    expr: Box::new(expr),
                })
            }
            _ => Err(Error::Syntax(format!(
                "Unexpected token in expression: {:?}",
                self.current_token
            ))),
        }
    }

    fn parse_infix_expr(&mut self, left: Expr) -> Result<Expr, Error> {
        match &self.current_token {
            Token::Plus | Token::Minus | Token::Multiply | Token::Divide | Token::Modulo |
            Token::Equals | Token::NotEquals | Token::Less | Token::Greater |
            Token::LessEqual | Token::GreaterEqual | Token::And | Token::Or => {
                let op = self.parse_binary_op()?;
                let precedence = self.get_precedence(&self.current_token);
                self.next_token()?;
                let right = self.parse_expr(precedence)?;
                Ok(Expr::Binary {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                })
            }
            Token::Is => {
                self.next_token()?;
                if matches!(self.current_token, Token::Not) {
                    self.next_token()?;
                    if matches!(self.current_token, Token::Null) {
                        self.next_token()?;
                        Ok(Expr::Unary {
                            op: UnaryOp::IsNotNull,
                            expr: Box::new(left),
                        })
                    } else {
                        Err(Error::Syntax("Expected NULL after IS NOT".to_string()))
                    }
                } else if matches!(self.current_token, Token::Null) {
                    self.next_token()?;
                    Ok(Expr::Unary {
                        op: UnaryOp::IsNull,
                        expr: Box::new(left),
                    })
                } else {
                    Err(Error::Syntax("Expected NULL or NOT NULL after IS".to_string()))
                }
            }
            _ => Err(Error::Syntax(format!(
                "Unexpected token in infix expression: {:?}",
                self.current_token
            ))),
        }
    }

    fn parse_binary_op(&self) -> Result<BinaryOp, Error> {
        match &self.current_token {
            Token::Plus => Ok(BinaryOp::Add),
            Token::Minus => Ok(BinaryOp::Subtract),
            Token::Multiply => Ok(BinaryOp::Multiply),
            Token::Divide => Ok(BinaryOp::Divide),
            Token::Modulo => Ok(BinaryOp::Modulo),
            Token::Equals => Ok(BinaryOp::Eq),
            Token::NotEquals => Ok(BinaryOp::NotEq),
            Token::Less => Ok(BinaryOp::Lt),
            Token::Greater => Ok(BinaryOp::Gt),
            Token::LessEqual => Ok(BinaryOp::LtEq),
            Token::GreaterEqual => Ok(BinaryOp::GtEq),
            Token::And => Ok(BinaryOp::And),
            Token::Or => Ok(BinaryOp::Or),
            _ => Err(Error::Syntax(format!(
                "Expected binary operator, got {:?}",
                self.current_token
            ))),
        }
    }

    fn parse_group_by(&mut self) -> Result<Vec<Expr>, Error> {
        if matches!(self.current_token, Token::Group) {
            self.next_token()?;
            self.expect_token(Token::By)?;
            self.parse_expr_list()
        } else {
            Ok(Vec::new())
        }
    }

    fn parse_having(&mut self) -> Result<Option<Expr>, Error> {
        if matches!(self.current_token, Token::Having) {
            self.next_token()?;
            Ok(Some(self.parse_expr(0)?))
        } else {
            Ok(None)
        }
    }

    fn parse_order_by(&mut self) -> Result<Vec<OrderByExpr>, Error> {
        if matches!(self.current_token, Token::Order) {
            self.next_token()?;
            self.expect_token(Token::By)?;
            
            let mut order_by = Vec::new();
            loop {
                let expr = self.parse_expr(0)?;
                let asc = if matches!(self.current_token, Token::Desc) {
                    self.next_token()?;
                    false
                } else if matches!(self.current_token, Token::Asc) {
                    self.next_token()?;
                    true
                } else {
                    true
                };
                
                let nulls_first = if matches!(self.current_token, Token::Nulls) {
                    self.next_token()?;
                    match self.current_token {
                        Token::First => {
                            self.next_token()?;
                            true
                        }
                        Token::Last => {
                            self.next_token()?;
                            false
                        }
                        _ => return Err(Error::Syntax("Expected FIRST or LAST after NULLS".to_string())),
                    }
                } else {
                    // Default NULLS LAST
                    false
                };
                
                order_by.push(OrderByExpr {
                    expr,
                    asc,
                    nulls_first,
                });
                
                if !matches!(self.current_token, Token::Comma) {
                    break;
                }
                self.next_token()?;
            }
            
            Ok(order_by)
        } else {
            Ok(Vec::new())
        }
    }

    fn parse_limit(&mut self) -> Result<Option<LimitClause>, Error> {
        if matches!(self.current_token, Token::Limit) {
            self.next_token()?;
            let limit = match &self.current_token {
                Token::Number(n) => n.parse().map_err(|_| {
                    Error::Syntax("Invalid LIMIT value".to_string())
                })?,
                _ => return Err(Error::Syntax("Expected number after LIMIT".to_string())),
            };
            self.next_token()?;
            
            let offset = if matches!(self.current_token, Token::Offset) {
                self.next_token()?;
                match &self.current_token {
                    Token::Number(n) => {
                        let offset = n.parse().map_err(|_| {
                            Error::Syntax("Invalid OFFSET value".to_string())
                        })?;
                        self.next_token()?;
                        Some(offset)
                    }
                    _ => return Err(Error::Syntax("Expected number after OFFSET".to_string())),
                }
            } else {
                None
            };
            
            Ok(Some(LimitClause { limit, offset }))
        } else {
            Ok(None)
        }
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>, Error> {
        let mut exprs = Vec::new();
        loop {
            exprs.push(self.parse_expr(0)?);
            if !matches!(self.current_token, Token::Comma) {
                break;
            }
            self.next_token()?;
        }
        Ok(exprs)
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<String>, Error> {
        let mut idents = Vec::new();
        loop {
            match &self.current_token {
                Token::Identifier(name) => {
                    idents.push(name.clone());
                    self.next_token()?;
                }
                _ => return Err(Error::Syntax("Expected identifier".to_string())),
            }
            if !matches!(self.current_token, Token::Comma) {
                break;
            }
            self.next_token()?;
        }
        Ok(idents)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_basic() {
        let input = "SELECT id, name FROM users";
        let mut parser = Parser::new(input).unwrap();
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(select) => {
                assert_eq!(select.columns.len(), 2);
                assert_eq!(select.from.name, "users");
                assert!(select.where_clause.is_none());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_select_where() {
        let input = "SELECT * FROM users WHERE age > 18";
        let mut parser = Parser::new(input).unwrap();
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(select) => {
                assert!(select.where_clause.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_select_complex() {
        let input = "SELECT u.id, u.name, COUNT(*) as count \
                    FROM users u \
                    JOIN orders o ON u.id = o.user_id \
                    WHERE u.age > 18 \
                    GROUP BY u.id, u.name \
                    HAVING COUNT(*) > 5 \
                    ORDER BY count DESC \
                    LIMIT 10 OFFSET 20";
        let mut parser = Parser::new(input).unwrap();
        parser.parse_statement().unwrap();
    }
}