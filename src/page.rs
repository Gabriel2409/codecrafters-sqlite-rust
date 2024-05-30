use binrw::binrw;

// https://www.sqlite.org/fileformat.html

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
    left_child_pointer: u32,
    /// TODO: modify this
    integer_key: u8,
}
