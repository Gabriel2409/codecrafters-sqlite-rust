use nom::{
    branch::alt,
    bytes::complete::{tag, take_until, take_while1},
    character::complete::{
        self, alpha1, alphanumeric1, char, line_ending, none_of, not_line_ending, one_of, space0,
        space1,
    },
    multi::{separated_list0, separated_list1},
    sequence::delimited,
    IResult,
};

#[derive(Debug, Clone)]
pub struct SelectQuery {
    pub columns: Vec<String>,
    pub tablename: String,
}

fn parse_identifier(input: &str) -> IResult<&str, &str> {
    delimited(
        space0,
        take_while1(|c: char| c == '(' || c == ')' || c == '*' || c.is_alphanumeric()),
        space0,
    )(input)
}

fn parse_columns(input: &str) -> IResult<&str, Vec<&str>> {
    separated_list0(delimited(space0, char(','), space0), parse_identifier)(input)
}

pub fn parse_select_command(input: &str) -> IResult<&str, SelectQuery> {
    let (input, _) = tag("SELECT")(input)?;
    let (input, columns) = parse_columns(input)?;
    let columns = columns
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let (input, _) = space0(input)?;
    let (input, _) = tag("FROM")(input)?;

    let (input, tablename) = parse_identifier(input)?;
    let tablename = tablename.to_string();
    let (input, _) = tag(";")(input)?;

    let select_query = SelectQuery { columns, tablename };

    Ok((input, select_query))
}
