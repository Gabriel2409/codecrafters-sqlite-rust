use std::{
    collections::HashMap,
    io::{Read, Seek},
};

use binrw::{binrw, BinRead, BinResult, BinWrite};

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
    // End of header
}

/// After the header, a page is followed by a pointer arraya
/// The cell pointer array consists of K 2-byte integer offsets to the cell contents
#[derive(Debug)]
#[binrw]
#[brw(big)]
#[br(import { nb_cells: u16 })]
pub struct PageCellPointerArray {
    #[br(count = nb_cells)]
    pub pointer_cell_array: Vec<u16>,
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

#[derive(Debug)]
#[binrw]
#[brw(big)]
pub struct BTreeTableLeafCell {
    #[br(parse_with = parse_varint)]
    pub nb_bytes_key_payload_including_overflow: u64,
    #[br(parse_with = parse_varint)]
    pub integer_key: u64,
    #[br(count = nb_bytes_key_payload_including_overflow)]
    pub payload: Vec<u8>,
    // REST not parsed
}

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
