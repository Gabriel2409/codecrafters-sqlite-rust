mod database_header;
mod page;

use anyhow::{bail, Result};
use binrw::BinRead;
use clap::{Parser, Subcommand};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    os::unix::fs::FileExt,
};

use database_header::DatabaseHeader;
use page::{PageCellPointerArray, PageHeader, PageType};

use crate::page::BTreeTableInteriorCell;

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

fn get_nb_of_tables(file: &mut File, initial_pos: u16) -> Result<usize> {
    let page_header = PageHeader::read(file)?;
    dbg!(&page_header);
    let nb_of_tables = match page_header.page_type {
        PageType::InteriorTable => {
            let page_cell_pointer_array = PageCellPointerArray::read_args(
                file,
                binrw::args! {nb_cells: page_header.number_of_cells},
            )?;
            dbg!(&page_cell_pointer_array);

            file.seek(SeekFrom::Start(initial_pos as u64))?;
            for pointer in page_cell_pointer_array.pointer_cell_array {
                file.seek(SeekFrom::Current(pointer as i64))?;
                let b_tree_table_interior_cell = BTreeTableInteriorCell::read(file)?;
                dbg!(b_tree_table_interior_cell);
                file.seek(SeekFrom::Start(initial_pos as u64))?;
            }

            let total = page_header.number_of_cells;
            total
        }
        PageType::LeafTable => page_header.number_of_cells,
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

            let nb_of_tables = get_nb_of_tables(&mut file, 0)?;
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
