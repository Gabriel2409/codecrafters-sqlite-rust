mod database_header;
mod page_header;

use anyhow::{bail, Result};
use binrw::io::BufReader;
use binrw::BinRead;
use clap::{Parser, Subcommand};
use std::fs::File;
use std::io::{Cursor, Read};

use database_header::DatabaseHeader;

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

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::DbInfo => {
            let mut file = File::open(&cli.filename)?;

            let db_header = DatabaseHeader::read(&mut file)?;

            println!("database page size: {}", db_header.page_size);
        }
    }

    Ok(())
}
