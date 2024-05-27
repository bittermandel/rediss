use nom::{
    bytes::streaming::take,
    character::streaming::{crlf, digit1},
    combinator::{map_res, recognize},
    multi::many_m_n,
    sequence::terminated,
    IResult,
};

#[derive(Debug)]
pub enum RespType<'a> {
    Integer(usize),
    Array(Vec<RespType<'a>>),
    BulkString(&'a [u8]),
}

pub fn parse_command(input: &str) -> IResult<&str, RespType> {
    match &input[0..1] {
        "*" => parse_array(&input[1..]),
        "$" => parse_bulk_string(&input[1..]),
        ":" => parse_integer(&input[1..]),
        _ => unimplemented!(),
    }
}

pub fn parse_raw_integer(input: &str) -> IResult<&str, usize> {
    map_res(terminated(digit1, crlf), |s: &str| s.parse::<usize>())(input)
}

pub fn parse_integer(input: &str) -> IResult<&str, RespType> {
    map_res(recognize(digit1), |s: &str| {
        s.parse::<usize>().map(|v| RespType::Integer(v))
    })(input)
}

pub fn parse_array(input: &str) -> IResult<&str, RespType> {
    let (remainder, length) = parse_raw_integer(input)?;

    let (remainder, result) = many_m_n(length, length, parse_command)(remainder)?;
    Ok((remainder, RespType::Array(result)))
}

pub fn parse_bulk_string(input: &str) -> IResult<&str, RespType> {
    let (remainder, length) = parse_raw_integer(input)?;
    let (remainder, bytes) = terminated(take(length), crlf)(remainder)?;

    Ok((remainder, RespType::BulkString(bytes.as_bytes())))
}
