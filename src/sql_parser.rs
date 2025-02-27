use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_until, take_while1},
    character::complete::{char, multispace0, multispace1, space0},
    multi::{separated_list0, separated_list1},
    sequence::{delimited, preceded, separated_pair},
    IResult,
};

#[derive(Debug, Clone)]
pub struct SelectQuery {
    pub columns: Vec<String>,
    pub tablename: String,
    // compares column name to value
    pub where_clause: Option<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct CreateTableQuery {
    // names and types
    pub columns_and_types: Vec<Vec<String>>,
    pub tablename: String,
}

#[derive(Debug, Clone)]
pub struct CreateIndexQuery {
    pub indexname: String,
    pub colname: String,
    pub tablename: String,
}

fn parse_identifier(input: &str) -> IResult<&str, &str> {
    delimited(
        multispace0,
        alt((
            take_while1(|c: char| c == '_' || c.is_alphanumeric()),
            delimited(char('"'), take_until("\""), char('"')),
        )),
        multispace0,
    )(input)
}

fn parse_double_quote_value(input: &str) -> IResult<&str, &str> {
    delimited(char('\"'), take_until("'"), char('\''))(input)
}

fn parse_identifier_or_star(input: &str) -> IResult<&str, &str> {
    delimited(
        multispace0,
        take_while1(|c: char| {
            c == '(' || c == ')' || c == '*' || c == '\'' || c == '_' || c.is_alphanumeric()
        }),
        multispace0,
    )(input)
}

fn parse_columns(input: &str) -> IResult<&str, Vec<&str>> {
    separated_list0(
        delimited(multispace0, char(','), multispace0),
        parse_identifier_or_star,
    )(input)
}

fn parse_value(input: &str) -> IResult<&str, &str> {
    delimited(char('\''), take_until("'"), char('\''))(input)
}

fn parse_where_clause(input: &str) -> IResult<&str, (&str, &str)> {
    preceded(
        tag_no_case("WHERE"),
        delimited(
            multispace1,
            separated_pair(
                parse_identifier,
                delimited(multispace0, char('='), multispace0),
                parse_value,
            ),
            multispace0,
        ),
    )(input)
}

pub fn parse_select_command(input: &str) -> IResult<&str, SelectQuery> {
    let (input, _) = tag_no_case("SELECT")(input)?;
    let (input, columns) = parse_columns(input)?;
    let columns = columns
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let (input, _) = space0(input)?;
    let (input, _) = tag_no_case("FROM")(input)?;

    let (input, tablename) = parse_identifier(input)?;
    let tablename = tablename.to_string();

    let (_, where_clause) = parse_where_clause(input).ok().unzip();

    let where_clause = where_clause.map(|(a, b)| (a.to_owned(), b.to_owned()));
    // let (input, _) = tag(";")(input)?;

    let select_query = SelectQuery {
        columns,
        tablename,
        where_clause,
    };

    Ok((input, select_query))
}

fn parse_column_def(input: &str) -> IResult<&str, Vec<&str>> {
    separated_list1(
        multispace1,
        // alphanumeric1
        take_while1(|c: char| c == '_' || c.is_alphanumeric()),
    )(input)
}

fn parse_column_defs(input: &str) -> IResult<&str, Vec<Vec<&str>>> {
    separated_list0(
        tag(","),
        delimited(multispace0, parse_column_def, multispace0),
    )(input)
}

// "CREATE TABLE apples\n(\n\tid integer primary key autoincrement,\n\tname text,\n\tcolor text\n)"

pub fn parse_create_table_command(input: &str) -> IResult<&str, CreateTableQuery> {
    let (input, _) = tag_no_case("CREATE TABLE")(input)?;
    let (input, tablename) = parse_identifier(input)?;
    let tablename = tablename.to_string();
    let (input, _) = tag_no_case("(")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, columns_and_types) = parse_column_defs(input)?;

    let columns_and_types: Vec<Vec<String>> = columns_and_types
        .into_iter()
        .map(|inner_vec| inner_vec.into_iter().map(|s| s.to_string()).collect())
        .collect();

    let create_table_query = CreateTableQuery {
        columns_and_types,
        tablename,
    };
    Ok((input, create_table_query))
}

// CREATE INDEX idx_companies_country on companies (country)
pub fn parse_create_index_command(input: &str) -> IResult<&str, CreateIndexQuery> {
    let (input, _) = tag_no_case("CREATE INDEX")(input)?;
    let (input, indexname) = parse_identifier(input)?;
    let indexname = indexname.to_string();
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("on")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, tablename) = parse_identifier(input)?;
    let tablename = tablename.to_string();
    let (input, _) = tag_no_case("(")(input)?;
    let (input, _) = multispace0(input)?;
    let (input, colname) = parse_identifier(input)?;
    let colname = colname.to_string();

    let create_index_query = CreateIndexQuery {
        indexname,
        tablename,
        colname,
    };
    Ok((input, create_index_query))
}
