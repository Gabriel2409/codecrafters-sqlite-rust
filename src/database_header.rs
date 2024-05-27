use std::io::Read;

use binrw::binrw;

#[derive(Debug)]
#[binrw]
#[brw(big)]
pub struct DatabaseHeader {
    #[br(assert(String::from_utf8_lossy(&magic_string) == "SQLite format 3\0"))]
    #[br(count = 16)]
    pub magic_string: Vec<u8>,
    pub page_size: u16,
    pub file_format_write_version: u8, // 1 for legacy, 2 for WAL
    pub file_format_read_version: u8,  // 1 for legacy, 2 for WAL
    pub bytes_unused_reserved_space: u8,
    #[br(assert(max_embedded_payload_fraction == 64))]
    pub max_embedded_payload_fraction: u8,
    #[br(assert(min_embedded_payload_fraction == 32))]
    pub min_embedded_payload_fraction: u8,
    #[br(assert(leaf_payload_fraction == 32))]
    pub leaf_payload_fraction: u8,
    pub file_change_counter: u32,
    pub in_header_db_size: u32,
    pub page_no_first_freelink_trunk_page: u32,
    pub total_no_freelist_pages: u32,
    pub schema_cookie: u32,
    #[br(assert((1..=4).contains(&schema_format_number)))]
    pub schema_format_number: u32,
    pub default_page_cache_size: u32,
    pub largest_root_b_tree_page_number_auto_incremental_vacuum: u32,
    pub db_text_encoding: u32, //  1 means UTF-8. 2 means UTF-16le. 3 means UTF-16be.
    pub user_version: u32,
    pub incremental_vacuum_mode: u32, //  True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
    pub application_id: u32,
    #[br(count = 20)]
    #[br(assert(vector_all_zeros(&reserved)))]
    pub reserved: Vec<u8>, // should be all 0
    pub version_valid_for_number: u32,
    pub sqlite_version_number: u32,
}

fn vector_all_zeros(vector: &[u8]) -> bool {
    for &element in vector {
        if element != 0 {
            return false;
        }
    }
    true
}
