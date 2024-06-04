use std::any;

use binrw::{binrw, BinRead, BinResult};

// https://www.sqlite.org/fileformat.html

/// A page starts with a header
#[derive(Debug)]
#[binrw]
#[brw(big)]
pub struct PageHeader {
    pub page_type: PageType,
    pub start_first_freeblock_on_page: u16,
    pub number_of_cells: u16,
    /// A zero value for this integer is interpreted as 65536
    pub start_cell_content_area: u16,
    pub number_of_fragmented_free_bytes_in_cell_content_area: u8,
    /// binrw does not parse this field if the condition is not met, which means we
    /// don't advance the cursor
    #[br(if(page_type == PageType::InteriorTable || page_type == PageType::InteriorIndex))]
    pub right_most_pointer: u32,
}

#[derive(Debug, PartialEq)]
#[binrw]
pub enum PageType {
    #[brw(magic = 2u8)]
    InteriorIndex,
    #[brw(magic = 5u8)]
    InteriorTable,
    #[brw(magic = 10u8)]
    LeafIndex,
    #[brw(magic = 13u8)]
    LeafTable,
}

/// After the header, a page is followed by a pointer array
/// The cell pointer array consists of K 2-byte integer offsets to the cell contents
#[derive(Debug)]
#[binrw]
#[brw(big)]
#[br(import { nb_cells: usize })]
pub struct PageCellPointerArray {
    /// Offsets to cell content relative to the beginning of the page
    #[br(count = nb_cells)]
    pub offsets: Vec<u16>,
}

#[derive(Debug)]
#[binrw]
#[brw(big)]
pub struct BTreeTableInteriorCell {
    pub left_child_pointer: u32,
    /// A varint is between 1 and 9 bytes in length. The varint consists of either zero or more
    /// bytes which have the high-order bit set followed by a single byte with the high-order bit
    /// clear, or nine bytes, whichever is shorter.
    #[br(parse_with = parse_varint)]
    pub integer_key: u64,
}

/// NOTE: not fully parsed, still have to figure out how to differentiate
/// the payload and the 4-byte big-endian integer page number for the
/// first page of the overflow page list
#[derive(Debug)]
#[binrw]
#[brw(big)]
pub struct BTreeTableLeafCell {
    #[br(parse_with = parse_varint)]
    pub nb_bytes_key_payload_including_overflow: u64,
    #[br(parse_with = parse_varint)]
    pub integer_key: u64,
    pub record: Record,
    #[br(count = nb_bytes_key_payload_including_overflow)]
    /// initial portion of the payload that does not spill to overflow pages
    pub payload: Vec<u8>,
    // REST not parsed
}

/// TODO: actually parse the record to improve the BTreeTableLeafCell
/// a single record, see
/// https://www.sqlite.org/fileformat2.html#record_format
#[derive(Debug)]
#[binrw]
#[brw(big)]
pub struct Record {
    #[br(parse_with = parse_record_header)]
    pub column_type_varints: Vec<RecordType>,
}

#[binrw]
#[brw(big)]
#[derive(Debug, Clone)]
pub enum RecordType {
    NullRecord,
    Int8Record,
    Int16Record,
    Int24Record,
    Int32Record,
    Int48Record,
    Int64Record,
    Float64Record,
    Integer0,
    Integer1,
    Reserved,
    Blob(u64),
    String(u64),
}

impl TryFrom<u64> for RecordType {
    type Error = binrw::Error;

    fn try_from(serial_type: u64) -> Result<Self, Self::Error> {
        Ok(match serial_type {
            0 => RecordType::NullRecord,
            1 => RecordType::Int8Record,
            2 => RecordType::Int16Record,
            3 => RecordType::Int24Record,
            4 => RecordType::Int32Record,
            5 => RecordType::Int48Record,
            6 => RecordType::Int64Record,
            7 => RecordType::Float64Record,
            8 => RecordType::Integer0,
            9 => RecordType::Integer1,
            n if n == 10 || n == 11 => RecordType::Reserved,
            n if n >= 12 && n % 2 == 0 => RecordType::Blob((n - 12) / 2),
            n if n >= 13 && n % 2 == 1 => RecordType::String((n - 13) / 2),
            x => {
                return Err(binrw::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Could not convert varint {} to record type", x),
                )))
            }
        })
    }
}

/// Helper function to parse varint fields
#[binrw::parser(reader, endian)]
fn parse_varint() -> BinResult<u64> {
    let mut result = 0u64;
    let mut shift = 0;
    for _ in 0..9 {
        let byte = u8::read_options(reader, endian, ())?;
        result |= ((byte & 0x7F) as u64) << (7 * shift);
        if (byte & 0x80) == 0 {
            break;
        }
        shift += 1;
    }
    Ok(result)
}

#[binrw::parser(reader, endian)]
fn parse_varint_with_bytes() -> BinResult<(u64, usize)> {
    let mut result = 0u64;
    let mut shift = 0;
    let mut bytes_read = 0;
    for _ in 0..9 {
        let byte = u8::read_options(reader, endian, ())?;
        bytes_read += 1;
        result |= ((byte & 0x7F) as u64) << (7 * shift);
        if (byte & 0x80) == 0 {
            break;
        }
        shift += 1;
    }
    Ok((result, bytes_read))
}

#[binrw::parser(reader, endian)]
fn parse_record_header() -> BinResult<Vec<RecordType>> {
    let (nb_bytes_record_header, header_bytes_read) = parse_varint_with_bytes(reader, endian, ())?;

    let mut records_type = Vec::new();
    let mut total_bytes_read = header_bytes_read as u64;
    while total_bytes_read < nb_bytes_record_header {
        let (varint, bytes_read) = parse_varint_with_bytes(reader, endian, ())?;
        let record_type = RecordType::try_from(varint)?;
        records_type.push(record_type);
        total_bytes_read += bytes_read as u64;
    }

    Ok(records_type)
}
