// src/lexer.rs
use std::iter::Peekable;
use std::str::Chars;
use crate::error::Error;

#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    // Keywords
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    Table,
    Into,
    Values,
    From,
    Where,
    Group,
    Having,
    Order,
    By,
    Limit,
    Offset,
    And,
    Or,
    Not,
    Like,
    In,
    Between,
    Case,
    When,
    Then,
    Else,
    End,
    Null,
    Is,
    True,
    False,
    Primary,
    Foreign,
    Key,
    References,
    Unique,
    Check,
    Default,
    LeftJoin,
    RightJoin,
    FullJoin,
    
    // Identifiers and literals
    Identifier(String),
    String(String),
    Number(String),
    
    // Operators
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Equals,
    NotEquals,
    Less,
    Greater,
    LessEqual,
    GreaterEqual,
    
    // Delimiters
    Comma,
    Semicolon,
    LeftParen,
    RightParen,
    Period,
    
    // Special
    EOF,
}

pub struct Lexer<'a> {
    input: Peekable<Chars<'a>>,
    position: usize,
    line: usize,
    column: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Lexer {
            input: input.chars().peekable(),
            position: 0,
            line: 1,
            column: 1,
        }
    }
    
    pub fn next_token(&mut self) -> Result<Token, Error> {
        self.skip_whitespace();
        
        match self.peek() {
            None => Ok(Token::EOF),
            Some(c) => match c {
                'A'..='Z' | 'a'..='z' | '_' => self.read_identifier(),
                '0'..='9' => self.read_number(),
                '\'' | '"' => self.read_string(),
                '+' => self.single_char_token(Token::Plus),
                '-' => self.single_char_token(Token::Minus),
                '*' => self.single_char_token(Token::Multiply),
                '/' => self.single_char_token(Token::Divide),
                '%' => self.single_char_token(Token::Modulo),
                '=' => self.single_char_token(Token::Equals),
                ',' => self.single_char_token(Token::Comma),
                ';' => self.single_char_token(Token::Semicolon),
                '(' => self.single_char_token(Token::LeftParen),
                ')' => self.single_char_token(Token::RightParen),
                '.' => self.single_char_token(Token::Period),
                '<' => self.read_comparison_operator('<'),
                '>' => self.read_comparison_operator('>'),
                '!' => self.read_not_operator(),
                _ => Err(Error::Syntax(format!("Unexpected character: {}", c))),
            }
        }
    }
    
    fn peek(&mut self) -> Option<char> {
        self.input.peek().copied()
    }
    
    fn next(&mut self) -> Option<char> {
        let c = self.input.next();
        if let Some(ch) = c {
            self.position += 1;
            self.column += 1;
            if ch == '\n' {
                self.line += 1;
                self.column = 1;
            }
        }
        c
    }
    
    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek() {
            if !c.is_whitespace() {
                break;
            }
            self.next();
        }
    }
    
    fn read_identifier(&mut self) -> Result<Token, Error> {
        let mut identifier = String::new();
        
        while let Some(c) = self.peek() {
            if !c.is_alphanumeric() && c != '_' {
                break;
            }
            identifier.push(self.next().unwrap());
        }
        
        Ok(match identifier.to_uppercase().as_str() {
            "SELECT"     => Token::Select,
            "INSERT"     => Token::Insert,
            "UPDATE"     => Token::Update,
            "DELETE"     => Token::Delete,
            "CREATE"     => Token::Create,
            "DROP"       => Token::Drop,
            "ALTER"      => Token::Alter,
            "TABLE"      => Token::Table,
            "INTO"       => Token::Into,
            "VALUES"     => Token::Values,
            "FROM"       => Token::From,
            "WHERE"      => Token::Where,
            "GROUP"      => Token::Group,
            "HAVING"     => Token::Having,
            "ORDER"      => Token::Order,
            "BY"         => Token::By,
            "LIMIT"      => Token::Limit,
            "OFFSET"     => Token::Offset,
            "AND"        => Token::And,
            "OR"         => Token::Or,
            "NOT"        => Token::Not,
            "LIKE"       => Token::Like,
            "IN"         => Token::In,
            "BETWEEN"    => Token::Between,
            "CASE"       => Token::Case,
            "WHEN"       => Token::When,
            "THEN"       => Token::Then,
            "ELSE"       => Token::Else,
            "END"        => Token::End,
            "NULL"       => Token::Null,
            "IS"         => Token::Is,
            "TRUE"       => Token::True,
            "FALSE"      => Token::False,
            "PRIMARY"    => Token::Primary,
            "FOREIGN"    => Token::Foreign,
            "KEY"        => Token::Key,
            "REFERENCES" => Token::References,
            "UNIQUE"     => Token::Unique,
            "CHECK"      => Token::Check,
            "DEFAULT"    => Token::Default,
            _ => Token::Identifier(identifier),
        })
    }

    fn read_number(&mut self) -> Result<Token, Error> {
        let mut number = String::new();
        let mut has_decimal = false;
        
        while let Some(c) = self.peek() {
            match c {
                '0'..='9' => {
                    number.push(self.next().unwrap());
                }
                '.' => {
                    if has_decimal {
                        return Err(Error::Syntax("Invalid number format: multiple decimal points".to_string()));
                    }
                    has_decimal = true;
                    number.push(self.next().unwrap());
                }
                'e' | 'E' => {
                    number.push(self.next().unwrap());
                    // Handle scientific notation
                    if let Some(next) = self.peek() {
                        if next == '+' || next == '-' {
                            number.push(self.next().unwrap());
                        }
                    }
                }
                _ => break,
            }
        }
        
        Ok(Token::Number(number))
    }

    fn read_string(&mut self) -> Result<Token, Error> {
        let quote = self.next().unwrap();
        let mut string = String::new();
        let mut escaped = false;
        
        while let Some(c) = self.next() {
            match (escaped, c) {
                (true, 'n') => {
                    string.push('\n');
                    escaped = false;
                }
                (true, 'r') => {
                    string.push('\r');
                    escaped = false;
                }
                (true, 't') => {
                    string.push('\t');
                    escaped = false;
                }
                (true, '\\') => {
                    string.push('\\');
                    escaped = false;
                }
                (true, '\'') => {
                    string.push('\'');
                    escaped = false;
                }
                (true, '"') => {
                    string.push('"');
                    escaped = false;
                }
                (true, _) => {
                    return Err(Error::Syntax(format!("Invalid escape sequence: \\{}", c)));
                }
                (false, '\\') => {
                    escaped = true;
                }
                (false, c) if c == quote => {
                    return Ok(Token::String(string));
                }
                (false, c) => {
                    string.push(c);
                }
            }
        }
        
        Err(Error::Syntax("Unterminated string literal".to_string()))
    }

    fn single_char_token(&mut self, token: Token) -> Result<Token, Error> {
        self.next();
        Ok(token)
    }

    fn read_comparison_operator(&mut self, c: char) -> Result<Token, Error> {
        self.next();
        match (c, self.peek()) {
            ('<', Some('=')) => {
                self.next();
                Ok(Token::LessEqual)
            }
            ('<', Some('>')) => {
                self.next();
                Ok(Token::NotEquals)
            }
            ('>', Some('=')) => {
                self.next();
                Ok(Token::GreaterEqual)
            }
            ('<', _) => Ok(Token::Less),
            ('>', _) => Ok(Token::Greater),
            _ => Err(Error::Syntax("Invalid comparison operator".to_string())),
        }
    }

    fn read_not_operator(&mut self) -> Result<Token, Error> {
        self.next();
        match self.peek() {
            Some('=') => {
                self.next();
                Ok(Token::NotEquals)
            }
            _ => Err(Error::Syntax("Expected '=' after '!'".to_string())),
        }
    }

    pub fn get_position(&self) -> (usize, usize) {
        (self.line, self.column)
    }

    pub fn get_context(&self, width: usize) -> String {
        // Helper function to get context around the current position for error reporting
        let start = self.position.saturating_sub(width);
        let end = (self.position + width).min(self.input.clone().count());
        format!("...{}...", self.input.clone().skip(start).take(end - start).collect::<String>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_tokens() {
        let mut lexer = Lexer::new("SELECT * FROM users WHERE id = 1;");
        assert_eq!(lexer.next_token().unwrap(), Token::Select);
        assert_eq!(lexer.next_token().unwrap(), Token::Multiply);
        assert_eq!(lexer.next_token().unwrap(), Token::From);
        assert_eq!(lexer.next_token().unwrap(), Token::Identifier("users".to_string()));
        assert_eq!(lexer.next_token().unwrap(), Token::Where);
        assert_eq!(lexer.next_token().unwrap(), Token::Identifier("id".to_string()));
        assert_eq!(lexer.next_token().unwrap(), Token::Equals);
        assert_eq!(lexer.next_token().unwrap(), Token::Number("1".to_string()));
        assert_eq!(lexer.next_token().unwrap(), Token::Semicolon);
        assert_eq!(lexer.next_token().unwrap(), Token::EOF);
    }

    #[test]
    fn test_string_literals() {
        let mut lexer = Lexer::new("'hello' \"world\"");
        assert_eq!(lexer.next_token().unwrap(), Token::String("hello".to_string()));
        assert_eq!(lexer.next_token().unwrap(), Token::String("world".to_string()));
    }

    #[test]
    fn test_numbers() {
        let mut lexer = Lexer::new("123 45.67 1.2e-3");
        assert_eq!(lexer.next_token().unwrap(), Token::Number("123".to_string()));
        assert_eq!(lexer.next_token().unwrap(), Token::Number("45.67".to_string()));
        assert_eq!(lexer.next_token().unwrap(), Token::Number("1.2e-3".to_string()));
    }
}