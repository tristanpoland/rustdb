// src/ast.rs
use std::fmt;
pub use crate::types::Value;

#[derive(Debug, PartialEq, Clone)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Create(CreateStatement),
    Drop(DropStatement),
    Alter(AlterStatement),
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectStatement {
    pub distinct: bool,
    pub columns: Vec<SelectColumn>,
    pub from: TableReference,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<LimitClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct InsertStatement {
    pub table: TableReference,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expr>>,
    pub on_duplicate: Option<Vec<(String, Expr)>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct UpdateStatement {
    pub table: TableReference,
    pub sets: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<LimitClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct DeleteStatement {
    pub table: TableReference,
    pub where_clause: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<LimitClause>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateStatement {
    pub temporary: bool,
    pub if_not_exists: bool,
    pub table: TableReference,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct DropStatement {
    pub temporary: bool,
    pub if_exists: bool,
    pub table: TableReference,
    pub cascade: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub struct AlterStatement {
    pub table: TableReference,
    pub actions: Vec<AlterAction>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum AlterAction {
    AddColumn(ColumnDef),
    DropColumn(String),
    ModifyColumn(ColumnDef),
    RenameColumn(String, String),
    AddConstraint(TableConstraint),
    DropConstraint(String),
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectColumn {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct TableReference {
    pub name: String,
    pub schema: Option<String>,
    pub alias: Option<String>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: TableReference,
    pub on: Option<Expr>,
    pub using: Option<Vec<String>>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Expr {
    Column(ColumnRef),
    Literal(Value),
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Function {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    Exists(Box<SelectStatement>),
    Subquery(Box<SelectStatement>),
    List(Vec<Expr>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnRef {
    pub name: String,
    pub table: Option<String>,
    pub schema: Option<String>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    And,
    Or,
    Like,
    NotLike,
    In,
    NotIn,
}

#[derive(Debug, PartialEq, Clone)]
pub enum UnaryOp {
    Not,
    Negative,
    IsNull,
    IsNotNull,
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub struct LimitClause {
    pub limit: u64,
    pub offset: Option<u64>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum DataType {
    Integer(Option<u32>),
    Float(Option<(u32, u32)>),
    Decimal(Option<(u32, u32)>),
    Char(Option<u32>),
    Varchar(Option<u32>),
    Text,
    Date,
    Time,
    DateTime,
    Timestamp,
    Boolean,
    Binary(Option<u32>),
    Json,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ColumnConstraint {
    NotNull,
    Null,
    PrimaryKey,
    Unique,
    Default(Expr),
    Check(Expr),
    ForeignKey {
        table: String,
        column: String,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum TableConstraint {
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    Check {
        name: Option<String>,
        expr: Expr,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum ReferentialAction {
    Restrict,
    Cascade,
    SetNull,
    NoAction,
    SetDefault,
}

// Display implementations for debug and error reporting
impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Select(_) => write!(f, "SELECT"),
            Statement::Insert(_) => write!(f, "INSERT"),
            Statement::Update(_) => write!(f, "UPDATE"),
            Statement::Delete(_) => write!(f, "DELETE"),
            Statement::Create(_) => write!(f, "CREATE"),
            Statement::Drop(_) => write!(f, "DROP"),
            Statement::Alter(_) => write!(f, "ALTER"),
        }
    }
}

impl fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOp::Add => write!(f, "+"),
            BinaryOp::Subtract => write!(f, "-"),
            BinaryOp::Multiply => write!(f, "*"),
            BinaryOp::Divide => write!(f, "/"),
            BinaryOp::Modulo => write!(f, "%"),
            BinaryOp::Eq => write!(f, "="),
            BinaryOp::NotEq => write!(f, "!="),
            BinaryOp::Lt => write!(f, "<"),
            BinaryOp::Gt => write!(f, ">"),
            BinaryOp::LtEq => write!(f, "<="),
            BinaryOp::GtEq => write!(f, ">="),
            BinaryOp::And => write!(f, "AND"),
            BinaryOp::Or => write!(f, "OR"),
            BinaryOp::Like => write!(f, "LIKE"),
            BinaryOp::NotLike => write!(f, "NOT LIKE"),
            BinaryOp::In => write!(f, "IN"),
            BinaryOp::NotIn => write!(f, "NOT IN"),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column(col) => write!(f, "{}", col.name),
            Expr::Literal(val) => write!(f, "{}", val),
            Expr::Binary { left, op, right } => write!(f, "({} {} {})", left, op, right),
            Expr::Unary { op, expr } => write!(f, "{}({})", op, expr),
            Expr::Function { name, args, .. } => {
                write!(f, "{}(", name)?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", arg)?;
                }
                write!(f, ")")
            }
            _ => write!(f, "..."),
        }
    }
}