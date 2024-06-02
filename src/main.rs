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
    let page_header = PageHeader::read(file)?;

    let nb_of_tables = match page_header.page_type {
        PageType::InteriorTable => {
            let page_cell_pointer_array = PageCellPointerArray::read_args(
                file,
                binrw::args! {nb_cells: page_header.number_of_cells.into()},
            )?;

            let mut total = 0;

            for pointer in page_cell_pointer_array.pointer_cell_array {
                file.seek(SeekFrom::Start(initial_pos))?;
                file.seek(SeekFrom::Current(pointer as i64))?;
                let b_tree_table_interior_cell = BTreeTableInteriorCell::read(file)?;

                let page_position =
                    page_size as u64 * (b_tree_table_interior_cell.left_child_pointer - 1) as u64;

                file.seek(SeekFrom::Start(page_position))?;
                total += get_nb_of_tables(file, page_position, page_size)?;
            }

            let page_position = page_size as u64 * (page_header.right_most_pointer - 1) as u64;

            file.seek(SeekFrom::Start(page_position))?;
            total += get_nb_of_tables(file, page_position, page_size)?;

            total
        }
        PageType::LeafTable => {
            let page_cell_pointer_array = PageCellPointerArray::read_args(
                file,
                binrw::args! {nb_cells: page_header.number_of_cells.into()},
            )?;

            let mut total = 0;
            for pointer in page_cell_pointer_array.pointer_cell_array {
                file.seek(SeekFrom::Start(initial_pos))?;
                file.seek(SeekFrom::Current(pointer as i64))?;
                let b_tree_table_leaf_cell = BTreeTableLeafCell::read(file)?;

                // Taking the number_of_cells of the page actually gives
                let begin_payload = String::from_utf8_lossy(&b_tree_table_leaf_cell.payload);
                if begin_payload.contains("CREATE TABLE") {
                    total += 1;
                }
                dbg!(String::from_utf8_lossy(&b_tree_table_leaf_cell.payload));
            }

            total
        }
        _ => anyhow::bail!("Invalid page type to get nb of tables"),
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

            // let page_header = PageHeader::read(&mut file)?;
            // dbg!(page_header);

            // dbg!(file.stream_position()?);

            // let mut buf = [0; 10];
            // file.read_exact(&mut buf)?;
            // dbg!(buf);
        }
    }

    Ok(())
}
