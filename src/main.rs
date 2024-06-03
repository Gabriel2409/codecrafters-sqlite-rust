mod database_header;
mod page;

use anyhow::{bail, Result};
use binrw::BinRead;
use clap::{Parser, Subcommand};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use database_header::DatabaseHeader;
use page::{BTreeTableLeafCell, PageCellPointerArray, PageHeader, PageType};

use page::BTreeTableInteriorCell;

use crate::page::Record;

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

/// Helper function to get the total number of tables.
/// For the sample.db, we can just read the number of cells in the page header.
/// However it does not work for more complex databases such as Chinook
/// (https://github.com/lerocha/chinook-database/releases):
/// the first page is not a LeafTable but an InteriorTable
/// In this case, the idea is to traverse the tree until we reach a LeafTable and
/// add the number of cells.
/// However, I noticed that it lead to an overestimation in the number of tables.
/// After investigation, it seems some of the cells correspond to an index
/// I added an extra filter that looks at the payload of the page and tries to locate
/// a CREATE TABLE statement. It seems to work but there is probably a better way
fn get_nb_of_tables(file: &mut File, initial_pos: u64, page_size: u16) -> Result<usize> {
    // When called for the first time, we are on the first page and already parsed the
    // database header: initial_pos is still 0 but file.stream_position() is 100.
    // For other pages, the page actually start with the page header, so the initial_pos
    // corresponds to file.stream_position()

    let page_header = PageHeader::read(file)?;

    let nb_of_tables = match page_header.page_type {
        PageType::InteriorTable => {
            let page_cell_pointer_array = PageCellPointerArray::read_args(
                file,
                binrw::args! {nb_cells: page_header.number_of_cells.into()},
            )?;

            let mut total = 0;

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
                total += get_nb_of_tables(file, page_position, page_size)?;
            }

            // Important: We need to also add the page referenced by the right_most_pointer
            let page_position = page_size as u64 * (page_header.right_most_pointer - 1) as u64;
            file.seek(SeekFrom::Start(page_position))?;
            total += get_nb_of_tables(file, page_position, page_size)?;

            total
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

            let mut total = 0;
            for offset in page_cell_pointer_array.offsets {
                file.seek(SeekFrom::Start(initial_pos + offset as u64))?;
                let b_tree_table_leaf_cell = BTreeTableLeafCell::read(file)?;

                let begin_payload = String::from_utf8_lossy(&b_tree_table_leaf_cell.payload);
                let record = Record::read(file)?;
                dbg!(&begin_payload);
                if begin_payload.contains("CREATE TABLE") {
                    total += 1;
                }
            }

            total
        }
        _ => anyhow::bail!(
            "When traversing the b tree, only interior and leaf TABLE pages should be encountered"
        ),
    };

    Ok(nb_of_tables as usize)
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::DbInfo => {
            let mut file = File::open(&cli.filename)?;

            let db_header = DatabaseHeader::read(&mut file)?;

            println!("database page size: {}", db_header.page_size);

            let nb_of_tables = get_nb_of_tables(&mut file, 0, db_header.page_size)?;
            println!("number of tables: {}", nb_of_tables);
        }
        Commands::Tables => {
            let mut file = File::open(&cli.filename)?;

            let db_header = DatabaseHeader::read(&mut file)?;

            println!("database page size: {}", db_header.page_size);

            let nb_of_tables = get_nb_of_tables(&mut file, 0, db_header.page_size)?;
            println!("number of tables: {}", nb_of_tables);
        }
    }
    Ok(())
}
