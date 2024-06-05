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

/// The page type is coded on a unique byte
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
/// For now, we will only handle cases without overflow
#[derive(Debug)]
#[binrw]
#[brw(big)]
pub struct BTreeTableLeafCell {
    #[br(parse_with = parse_varint)]
    pub nb_bytes_key_payload_including_overflow: u64,
    #[br(parse_with = parse_varint)]
    pub integer_key: u64,
    pub record_header: RecordHeader,
    // #[br(count = nb_bytes_key_payload_including_overflow)]
    // initial portion of the payload that does not spill to overflow pages
    // we suppose there is no overflow for now
    // pub payload: Vec<u8>,
    // REST not parsed - we suppose there is no overflow
}

/// TODO: actually parse the record to improve the BTreeTableLeafCell
/// a single record, see
/// https://www.sqlite.org/fileformat2.html#record_format
#[derive(Debug)]
#[binrw]
#[brw(big)]
pub struct RecordHeader {
    #[br(parse_with = parse_record_header)]
    pub column_type_varints: Vec<ObjectType>,
}

#[binrw]
#[brw(big)]
#[derive(Debug, Clone)]
pub enum ObjectType {
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

impl TryFrom<u64> for ObjectType {
    type Error = binrw::Error;

    fn try_from(serial_type: u64) -> Result<Self, Self::Error> {
        Ok(match serial_type {
            0 => ObjectType::NullRecord,
            1 => ObjectType::Int8Record,
            2 => ObjectType::Int16Record,
            3 => ObjectType::Int24Record,
            4 => ObjectType::Int32Record,
            5 => ObjectType::Int48Record,
            6 => ObjectType::Int64Record,
            7 => ObjectType::Float64Record,
            8 => ObjectType::Integer0,
            9 => ObjectType::Integer1,
            n if n == 10 || n == 11 => ObjectType::Reserved,
            n if n >= 12 && n % 2 == 0 => ObjectType::Blob((n - 12) / 2),
            n if n >= 13 && n % 2 == 1 => ObjectType::String((n - 13) / 2),
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
fn parse_record_header() -> BinResult<Vec<ObjectType>> {
    let (size_header, header_bytes_read) = parse_varint_with_bytes(reader, endian, ())?;

    let mut records_type = Vec::new();
    let mut total_bytes_read = header_bytes_read as u64;
    while total_bytes_read < size_header {
        let (varint, bytes_read) = parse_varint_with_bytes(reader, endian, ())?;
        // dbg!(varint, bytes_read);
        let record_type = ObjectType::try_from(varint)?;
        records_type.push(record_type);
        total_bytes_read += bytes_read as u64;
    }

    for record_type in &records_type {
        match record_type {
            ObjectType::NullRecord => {
                dbg!("NULL");
            }
            ObjectType::Int8Record => {
                let mut buf = [0u8; 1];
                reader.read_exact(&mut buf)?;
                let val = u8::from_be_bytes(buf);
                dbg!(val);
            }
            ObjectType::Int16Record => {
                let mut buf = [0u8; 2];
                reader.read_exact(&mut buf)?;
                let val = u16::from_be_bytes(buf);
                dbg!(val);
            }
            ObjectType::Int24Record => {
                let mut buf = [0u8; 3];
                reader.read_exact(&mut buf)?;
                let val: u32 = (buf[0] as u32) << 16 + (buf[1] as u32) << 8 + buf[2] as u32;
                dbg!(val);
            }
            ObjectType::Int32Record => {
                let mut buf = [0u8; 4];
                reader.read_exact(&mut buf)?;
                let val = u32::from_be_bytes(buf);
                dbg!(val);
            }
            ObjectType::Int48Record => {
                let mut buf = [0u8; 6];
                reader.read_exact(&mut buf)?;
                let val: u64 = (buf[0] as u64)
                    << 40 + (buf[1] as u64)
                    << 32 + (buf[2] as u64)
                    << 24 + (buf[3] as u64)
                    << 16 + (buf[4] as u64)
                    << 8 + (buf[5] as u64);
                dbg!(val);
            }
            ObjectType::Int64Record => {
                let mut buf = [0u8; 8];
                reader.read_exact(&mut buf)?;
                let val = u64::from_be_bytes(buf);
                dbg!(val);
            }
            ObjectType::Float64Record => {
                let mut buf = [0u8; 8];
                reader.read_exact(&mut buf)?;
                let val = f64::from_be_bytes(buf);
                dbg!(val);
            }
            ObjectType::Integer0 => {
                let val = 0;
                dbg!(val);
            }
            ObjectType::Integer1 => {
                let val = 1;
                dbg!(val);
            }
            ObjectType::Reserved => todo!(),
            ObjectType::Blob(x) => {
                let mut buf = vec![0u8; *x as usize];
                reader.read_exact(&mut buf)?;
                dbg!("BLOB");
            }
            ObjectType::String(x) => {
                let mut buf = vec![0u8; *x as usize];
                reader.read_exact(&mut buf)?;
                let val = String::from_utf8_lossy(&buf);
                dbg!(val);
            }
        }
    }

    // let columns =

    Ok(records_type)
}
