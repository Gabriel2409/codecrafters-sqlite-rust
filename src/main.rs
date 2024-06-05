mod database_header;
mod page;
mod schema_table;

use anyhow::{bail, Result};
use binrw::BinRead;
use clap::{Parser, Subcommand};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use database_header::DatabaseHeader;
use page::{BTreeTableLeafCell, PageCellPointerArray, PageHeader, PageType, Record};

use page::BTreeTableInteriorCell;

use crate::schema_table::SchemaTable;

#[derive(Parser)]
#[command(version, about="Custom sqlite", long_about=None )]
struct Cli {
    #[arg(help = "Name of the db. Fails if file does not exist")]
    filename: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[command(name = ".dbinfo", about = "Show status information about the database")]
    DbInfo,
    #[command(name = ".tables", about = "Prints the table names")]
    Tables,
}

/// Helper function to parse all the information of a table
/// For the sample.db, we can just read the number of cells in the page header.
/// However it does not work for more complex databases such as Chinook
/// (https://github.com/lerocha/chinook-database/releases):
/// the first page is not a LeafTable but an InteriorTable
/// In this case, the idea is to traverse the tree until we reach a LeafTable and
/// then parse the leaf cells
fn get_table_records(file: &mut File, initial_pos: u64, page_size: u16) -> Result<Vec<Record>> {
    // initial_pos can be different from current stream position. For ex, on the first page,
    // this should be called after parsing the db header:
    // initial_pos is still 0 but file.stream_position() is 100.
    // For other pages, the page actually start with the page header, so the initial_pos
    // corresponds to file.stream_position()

    let page_header = PageHeader::read(file)?;

    let records = match page_header.page_type {
        PageType::InteriorTable => {
            let page_cell_pointer_array = PageCellPointerArray::read_args(
                file,
                binrw::args! {nb_cells: page_header.number_of_cells.into()},
            )?;

            let mut records = Vec::new();

            // Here we read the pages corresponding to the pointer array.
            // sqlite pages start at 1, which is why we have the -1
            for offset in page_cell_pointer_array.offsets {
                // offset is relative to start of the page
                file.seek(SeekFrom::Start(initial_pos + offset as u64))?;
                let b_tree_table_interior_cell = BTreeTableInteriorCell::read(file)?;

                let page_position =
                    page_size as u64 * (b_tree_table_interior_cell.left_child_pointer - 1) as u64;

                file.seek(SeekFrom::Start(page_position))?;
                // traverse the b tree.
                let child_records = get_table_records(file, page_position, page_size)?;
                records.extend(child_records);
            }

            // Important: We need to also add the page referenced by the right_most_pointer
            let page_position = page_size as u64 * (page_header.right_most_pointer - 1) as u64;
            file.seek(SeekFrom::Start(page_position))?;
            let child_records = get_table_records(file, page_position, page_size)?;
            records.extend(child_records);
            records
        }
        PageType::LeafTable => {
            // For leaf table, I was tempted to simply read the number_of_cells but
            // it overestimated the result for the Chinook db
            // Instead, we can parse the pointer array and look at each individual
            // cell then check the payload for the CREATE TABLE string.
            // This seems to work...

            let page_cell_pointer_array = PageCellPointerArray::read_args(
                file,
                binrw::args! {nb_cells: page_header.number_of_cells.into()},
            )?;

            let mut records = Vec::new();
            for offset in page_cell_pointer_array.offsets {
                file.seek(SeekFrom::Start(initial_pos + offset as u64))?;
                let b_tree_table_leaf_cell = BTreeTableLeafCell::read(file)?;

                records.push(b_tree_table_leaf_cell.record);

                // let begin_payload = String::from_utf8_lossy(&b_tree_table_leaf_cell.payload);
                // dbg!(record);
                // dbg!(&begin_payload[..60]);
                // if begin_payload.contains("CREATE TABLE") {
                //     total += 1;
                // }
            }
            records
        }
        _ => anyhow::bail!(
            "When traversing the b tree, only interior and leaf TABLE pages should be encountered"
        ),
    };

    Ok(records)
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::DbInfo => {
            let mut file = File::open(&cli.filename)?;

            let db_header = DatabaseHeader::read(&mut file)?;

            println!("database page size: {}", db_header.page_size);

            let records = get_table_records(&mut file, 0, db_header.page_size)?;
            let schema_table = SchemaTable::try_from(records)?;
            let table_names = schema_table.get_table_names();
            println!("number of tables: {}", table_names.len());
        }
        Commands::Tables => {
            let mut file = File::open(&cli.filename)?;

            let db_header = DatabaseHeader::read(&mut file)?;

            let records = get_table_records(&mut file, 0, db_header.page_size)?;
            let schema_table = SchemaTable::try_from(records)?;
            let table_names = schema_table.get_table_names();
            println!("{}", table_names.join(" "));
        }
    }
    Ok(())
}
