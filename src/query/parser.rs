use nom::{
    IResult,
    bytes::complete::{tag, take_while1},
    character::complete::{space0, space1, digit1, char, alphanumeric1},
    sequence::{tuple, delimited, terminated, preceded},
    branch::alt,
    multi::{separated_list0, many0},
    combinator::{opt, map, value, recognize},
};
use crate::error::Error;

/// SQL value types that can be parsed
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

/// SQL condition expressions
#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    Equals(String, Value),
    NotEquals(String, Value),
    GreaterThan(String, Value),
    LessThan(String, Value),
    GreaterEquals(String, Value),
    LessEquals(String, Value),
    Between(String, Value, Value),
    Like(String, String),
    In(String, Vec<Value>),
    IsNull(String),
    IsNotNull(String),
    And(Vec<Condition>),
    Or(Vec<Condition>),
    Not(Box<Condition>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub column: String,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedQuery {
    Select {
        table: String,
        columns: Vec<String>,
        conditions: Vec<Condition>,
        order_by: Vec<OrderBy>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<Value>>,
    },
    Update {
        table: String,
        set: Vec<(String, Value)>,
        conditions: Vec<Condition>,
    },
    Delete {
        table: String,
        conditions: Vec<Condition>,
    },
    Create {
        table: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
    },
    Drop {
        table: String,
        if_exists: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub type_name: String,
    pub constraints: Vec<ColumnConstraint>,
    pub default: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
    PrimaryKey,
    Unique,
    NotNull,
    Check(Condition),
    ForeignKey {
        ref_table: String,
        ref_column: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    PrimaryKey(Vec<String>),
    Unique(Vec<String>),
    ForeignKey {
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
    },
    Check(Condition),
}

pub struct Parser;

impl Parser {
    pub fn new() -> Self {
        Self
    }

    /// Parse a SQL query string into a ParsedQuery
    pub fn parse(&self, input: &str) -> Result<ParsedQuery, Error> {
        let (_, query) = self.parse_query(input)
            .map_err(|e| Error::Parse(format!("Failed to parse query: {}", e)))?;
        Ok(query)
    }

    // Parser combinators

    fn parse_query(&self, input: &str) -> IResult<&str, ParsedQuery> {
        alt((
            self.parse_select,
            self.parse_insert,
            self.parse_update,
            self.parse_delete,
            self.parse_create,
            self.parse_drop,
        ))(input)
    }

    fn parse_select(&self, input: &str) -> IResult<&str, ParsedQuery> {
        let (input, _) = tag_no_case("SELECT")(input)?;
        let (input, _) = space1(input)?;
        let (input, columns) = self.parse_column_list(input)?;
        let (input, _) = space1(input)?;
        let (input, _) = tag_no_case("FROM")(input)?;
        let (input, _) = space1(input)?;
        let (input, table) = self.parse_identifier(input)?;
        let (input, conditions) = opt(preceded(
            tuple((space1, tag_no_case("WHERE"), space1)),
            self.parse_conditions,
        ))(input)?;
        let (input, order_by) = opt(preceded(
            tuple((space1, tag_no_case("ORDER"), space1, tag_no_case("BY"), space1)),
            self.parse_order_by,
        ))(input)?;
        let (input, limit) = opt(preceded(
            tuple((space1, tag_no_case("LIMIT"), space1)),
            map(self.parse_integer, |n| n as usize),
        ))(input)?;
        let (input, offset) = opt(preceded(
            tuple((space1, tag_no_case("OFFSET"), space1)),
            map(self.parse_integer, |n| n as usize),
        ))(input)?;

        Ok((input, ParsedQuery::Select {
            table,
            columns,
            conditions: conditions.unwrap_or_default(),
            order_by: order_by.unwrap_or_default(),
            limit,
            offset,
        }))
    }

    fn parse_insert(&self, input: &str) -> IResult<&str, ParsedQuery> {
        let (input, _) = tag_no_case("INSERT INTO")(input)?;
        let (input, _) = space1(input)?;
        let (input, table) = self.parse_identifier(input)?;
        let (input, columns) = delimited(
            char('('),
            self.parse_column_list,
            char(')'),
        )(input)?;
        let (input, _) = space1(input)?;
        let (input, _) = tag_no_case("VALUES")(input)?;
        let (input, values) = self.parse_value_lists(input)?;

        Ok((input, ParsedQuery::Insert {
            table,
            columns,
            values,
        }))
    }

    fn parse_update(&self, input: &str) -> IResult<&str, ParsedQuery> {
        let (input, _) = tag_no_case("UPDATE")(input)?;
        let (input, _) = space1(input)?;
        let (input, table) = self.parse_identifier(input)?;
        let (input, _) = space1(input)?;
        let (input, _) = tag_no_case("SET")(input)?;
        let (input, _) = space1(input)?;
        let (input, set) = self.parse_set_clauses(input)?;
        let (input, conditions) = opt(preceded(
            tuple((space1, tag_no_case("WHERE"), space1)),
            self.parse_conditions,
        ))(input)?;

        Ok((input, ParsedQuery::Update {
            table,
            set,
            conditions: conditions.unwrap_or_default(),
        }))
    }

    fn parse_delete(&self, input: &str) -> IResult<&str, ParsedQuery> {
        let (input, _) = tag_no_case("DELETE FROM")(input)?;
        let (input, _) = space1(input)?;
        let (input, table) = self.parse_identifier(input)?;
        let (input, conditions) = opt(preceded(
            tuple((space1, tag_no_case("WHERE"), space1)),
            self.parse_conditions,
        ))(input)?;

        Ok((input, ParsedQuery::Delete {
            table,
            conditions: conditions.unwrap_or_default(),
        }))
    }

    fn parse_create(&self, input: &str) -> IResult<&str, ParsedQuery> {
        let (input, _) = tag_no_case("CREATE TABLE")(input)?;
        let (input, _) = space1(input)?;
        let (input, table) = self.parse_identifier(input)?;
        let (input, _) = space0(input)?;
        let (input, (columns, constraints)) = delimited(
            char('('),
            tuple((
                terminated(self.parse_column_defs, opt(char(','))),
                many0(preceded(char(','), self.parse_table_constraint)),
            )),
            char(')'),
        )(input)?;

        Ok((input, ParsedQuery::Create {
            table,
            columns,
            constraints,
        }))
    }

    fn parse_drop(&self, input: &str) -> IResult<&str, ParsedQuery> {
        let (input, _) = tag_no_case("DROP TABLE")(input)?;
        let (input, if_exists) = opt(preceded(
            space1,
            value(true, tag_no_case("IF EXISTS")),
        ))(input)?;
        let (input, _) = space1(input)?;
        let (input, table) = self.parse_identifier(input)?;

        Ok((input, ParsedQuery::Drop {
            table,
            if_exists: if_exists.unwrap_or(false),
        }))
    }

    // Helper parsers

    fn parse_identifier(&self, input: &str) -> IResult<&str, String> {
        map(
            recognize(
                tuple((
                    alt((
                        alpha1,
                        tag("_")
                    )),
                    many0(alt((
                        alphanumeric1,
                        tag("_")
                    )))
                ))
            ),
            |s: &str| s.to_string()
        )(input)
    }

    fn parse_value_lists(&self, input: &str) -> IResult<&str, Vec<Vec<Value>>> {
        separated_list0(
            tuple((char(','), space0)),
            delimited(
                char('('),
                separated_list0(
                    tuple((char(','), space0)),
                    self.parse_value,
                ),
                char(')'),
            ),
        )(input)
    }

    fn parse_set_clauses(&self, input: &str) -> IResult<&str, Vec<(String, Value)>> {
        separated_list0(
            tuple((char(','), space0)),
            tuple((
                |i| self.parse_identifier(i),
                preceded(
                    tuple((space0, char('='), space0)),
                    self.parse_value,
                ),
            )),
        )(input)
    }

    fn parse_column_defs(&self, input: &str) -> IResult<&str, Vec<ColumnDef>> {
        separated_list0(
            tuple((char(','), space0)),
            |i| self.parse_column_def(i),
        )(input)
    }

    fn parse_column_def(&self, input: &str) -> IResult<&str, ColumnDef> {
        let (input, name) = self.parse_identifier(input)?;
        let (input, _) = space1(input)?;
        let (input, type_name) = self.parse_type(input)?;
        let (input, constraints) = many0(preceded(
            space1,
            self.parse_column_constraint,
        ))(input)?;
        let (input, default) = opt(preceded(
            tuple((space1, tag_no_case("DEFAULT"), space1)),
            self.parse_value,
        ))(input)?;

        Ok((input, ColumnDef {
            name,
            type_name,
            constraints,
            default,
        }))
    }

    fn parse_type(&self, input: &str) -> IResult<&str, String> {
        let base_type = alt((
            tag_no_case("INTEGER"),
            tag_no_case("INT"),
            tag_no_case("BIGINT"),
            tag_no_case("FLOAT"),
            tag_no_case("DOUBLE"),
            tag_no_case("TEXT"),
            tag_no_case("VARCHAR"),
            tag_no_case("BOOLEAN"),
            tag_no_case("TIMESTAMP"),
            tag_no_case("DATE"),
        ));

        let array_type = tuple((
            base_type,
            preceded(char('['), terminated(opt(self.parse_integer), char(']'))),
        ));

        map(
            alt((
                array_type,
                map(base_type, |t| (t, None)),
            )),
            |(type_name, size)| {
                if let Some(size) = size {
                    format!("{}[{}]", type_name, size)
                } else {
                    type_name.to_string()
                }
            },
        )(input)
    }

    fn parse_column_constraint(&self, input: &str) -> IResult<&str, ColumnConstraint> {
        alt((
            value(ColumnConstraint::PrimaryKey, tag_no_case("PRIMARY KEY")),
            value(ColumnConstraint::Unique, tag_no_case("UNIQUE")),
            value(ColumnConstraint::NotNull, tag_no_case("NOT NULL")),
            map(
                preceded(
                    tuple((tag_no_case("CHECK"), space0, char('('))),
                    terminated(self.parse_condition, char(')')),
                ),
                ColumnConstraint::Check,
            ),
            map(
                preceded(
                    tuple((
                        tag_no_case("REFERENCES"),
                        space1,
                    )),
                    tuple((
                        self.parse_identifier,
                        delimited(
                            char('('),
                            self.parse_identifier,
                            char(')'),
                        ),
                    )),
                ),
                |(ref_table, ref_column)| ColumnConstraint::ForeignKey {
                    ref_table,
                    ref_column,
                },
            ),
        ))(input)
    }

    fn parse_table_constraint(&self, input: &str) -> IResult<&str, TableConstraint> {
        alt((
            // Primary key constraint
            map(
                preceded(
                    tuple((tag_no_case("PRIMARY KEY"), space0, char('('))),
                    terminated(
                        separated_list0(
                            tuple((char(','), space0)),
                            self.parse_identifier,
                        ),
                        char(')'),
                    ),
                ),
                TableConstraint::PrimaryKey,
            ),
            // Unique constraint
            map(
                preceded(
                    tuple((tag_no_case("UNIQUE"), space0, char('('))),
                    terminated(
                        separated_list0(
                            tuple((char(','), space0)),
                            self.parse_identifier,
                        ),
                        char(')'),
                    ),
                ),
                TableConstraint::Unique,
            ),
            // Foreign key constraint
            map(
                preceded(
                    tuple((tag_no_case("FOREIGN KEY"), space0)),
                    tuple((
                        delimited(
                            char('('),
                            separated_list0(
                                tuple((char(','), space0)),
                                self.parse_identifier,
                            ),
                            char(')'),
                        ),
                        preceded(
                            tuple((space1, tag_no_case("REFERENCES"), space1)),
                            tuple((
                                self.parse_identifier,
                                delimited(
                                    char('('),
                                    separated_list0(
                                        tuple((char(','), space0)),
                                        self.parse_identifier,
                                    ),
                                    char(')'),
                                ),
                            )),
                        ),
                    )),
                ),
                |(columns, (ref_table, ref_columns))| TableConstraint::ForeignKey {
                    columns,
                    ref_table,
                    ref_columns,
                },
            ),
            // Check constraint
            map(
                preceded(
                    tuple((tag_no_case("CHECK"), space0, char('('))),
                    terminated(self.parse_condition, char(')')),
                ),
                TableConstraint::Check,
            ),
        ))(input)
    }

    fn parse_order_by(&self, input: &str) -> IResult<&str, Vec<OrderBy>> {
        separated_list0(
            tuple((char(','), space0)),
            map(
                tuple((
                    self.parse_identifier,
                    opt(preceded(
                        space1,
                        alt((
                            value(OrderDirection::Asc, tag_no_case("ASC")),
                            value(OrderDirection::Desc, tag_no_case("DESC")),
                        )),
                    )),
                )),
                |(column, direction)| OrderBy {
                    column,
                    direction: direction.unwrap_or(OrderDirection::Asc),
                },
            ),
        )(input)
    }
}

// Helper functions
fn tag_no_case(tag: &'static str) -> impl Fn(&str) -> IResult<&str, &str> {
    move |input: &str| {
        let (input, _) = space0(input)?;
        let mut it = input.chars();
        let mut tag_it = tag.chars();
        let mut matched = String::new();
        
        loop {
            match (it.next(), tag_it.next()) {
                (Some(ic), Some(tc)) => {
                    if ic.to_lowercase().next() != tc.to_lowercase().next() {
                        return Err(nom::Err::Error(nom::error::Error::new(
                            input,
                            nom::error::ErrorKind::Tag,
                        )));
                    }
                    matched.push(ic);
                }
                (Some(_), None) | (None, Some(_)) => {
                    return Err(nom::Err::Error(nom::error::Error::new(
                        input,
                        nom::error::ErrorKind::Tag,
                    )));
                }
                (None, None) => break,
            }
        }
        
        Ok((&input[matched.len()..], &input[..matched.len()]))
    }
}