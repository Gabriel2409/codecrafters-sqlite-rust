use nom::{
    bytes::complete::{tag, take_until},
    character::complete::{self, alpha1, char, line_ending, space0, space1},
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
    delimited(space0, alpha1, space0)(input)
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
