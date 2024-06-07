mod database_header;
mod page;
mod schema_table;
mod sql_parser;

use anyhow::Result;
use binrw::BinRead;
use clap::{Parser, Subcommand};
use sql_parser::parse_select_command;
use std::{
    fs::File,
    io::{Seek, SeekFrom},
};

use database_header::DatabaseHeader;
use page::{BTreeTableLeafCell, PageCellPointerArray, PageHeader, PageType, Record};

use page::BTreeTableInteriorCell;

use crate::{schema_table::SchemaTable, sql_parser::parse_create_table_command};

#[derive(Parser, Clone)]
#[command(version, about="Custom sqlite", long_about=None )]
struct Cli {
    #[arg(help = "Name of the db. Fails if file does not exist")]
    filename: String,

    #[arg(help = "SQL command to execute")]
    sql_command: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Clone)]
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
                let cell_position = initial_pos + offset as u64;
                file.seek(SeekFrom::Start(cell_position))?;
                let b_tree_table_leaf_cell = BTreeTableLeafCell::read(file)?;

                records.push(b_tree_table_leaf_cell.record);
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

    // needs the finish keyword to avoid lifetime erros
    let mut is_sql_command = false;
    if let Some(sql_command) = &cli.sql_command {
        is_sql_command = true;
        match parse_select_command(sql_command) {
            Ok((_, select_query)) => {
                let mut file = File::open(&cli.filename)?;

                let db_header = DatabaseHeader::read(&mut file)?;

                let records = get_table_records(&mut file, 0, db_header.page_size)?;
                let schema_table = SchemaTable::try_from(records)?;

                let table_record = schema_table
                    .get_schema_record_for_table(&select_query.tablename)
                    .expect("Could not find table");

                let root_page = table_record.rootpage;
                let col_names = match parse_create_table_command(&table_record.sql) {
                    Ok((_, create_table_query)) => {
                        assert_eq!(
                            &create_table_query.tablename.to_lowercase(),
                            &select_query.tablename.to_lowercase()
                        );
                        create_table_query
                            .columns_and_types
                            .into_iter()
                            .map(|c| c[0].clone())
                            .collect::<Vec<_>>()
                    }
                    Err(_) => {
                        anyhow::bail!("Error parsing SQL command")
                    }
                };

                let page_position = db_header.page_size as u64 * (root_page - 1) as u64;
                file.seek(SeekFrom::Start(page_position))?;
                let records = get_table_records(&mut file, page_position, db_header.page_size)?;

                dbg!(&select_query);
                if select_query.columns.len() == 1
                    && select_query.columns[0].to_lowercase() == "count(*)"
                {
                    println!("{}", records.len());
                } else {
                    let mut kept_cols = Vec::new();

                    let mut where_col = None;
                    let mut where_val = String::from("");
                    let mut id_col = None;
                    for column in &select_query.columns {
                        for (i, col) in col_names.iter().enumerate() {
                            if column.to_lowercase() == col.to_lowercase() {
                                kept_cols.push(i);
                            }
                            // TODO: make a better paser, this is wrong
                            if col == "id" {
                                id_col = Some(i);
                            }
                            if let Some(where_clause) = &select_query.where_clause {
                                if col.to_lowercase() == where_clause.0.to_lowercase() {
                                    where_val = where_clause.1.clone();
                                    where_col = Some(i);
                                }
                            }
                        }
                    }

                    for record in records {
                        let mut cur_recs = Vec::new();
                        if let Some(where_col) = where_col {
                            let mut column_repr = record.column_contents[where_col].repr();
                            if id_col == Some(where_col) {
                                column_repr = format!("{}", record.integer_key);
                            }

                            if where_val != column_repr {
                                continue;
                            }
                        }

                        for kept_col in &kept_cols {
                            let mut column_repr = record.column_contents[*kept_col].repr();
                            if id_col == Some(*kept_col) {
                                column_repr = format!("{}", record.integer_key);
                            }
                            cur_recs.push(column_repr);
                        }
                        println!("{}", cur_recs.join("|"));
                    }
                }
            }
            Err(x) => {
                anyhow::bail!("Error parsing SQL command")
            }
        };
    }

    if is_sql_command {
        return Ok(());
    }

    match &cli.command.expect("Should have a command at this point") {
        Commands::DbInfo => {
            let mut file = File::open(&cli.filename)?;

            let db_header = DatabaseHeader::read(&mut file)?;

            println!("database page size: {}", db_header.page_size);

            let records = get_table_records(&mut file, 0, db_header.page_size)?;
            let schema_table = SchemaTable::try_from(records)?;
            let nb_tables = schema_table.get_nb_tables();
            println!("number of tables: {}", nb_tables);
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
