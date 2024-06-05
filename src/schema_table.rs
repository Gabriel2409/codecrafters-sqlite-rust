use anyhow::Context;

use crate::page::{ColumnContent, Record};

/// https://sqlite.org/schematab.html

#[derive(Debug)]
pub struct SchemaTable {
    records: Vec<SchemaTableRecord>,
}

impl SchemaTable {
    pub fn get_table_names(&self) -> Vec<String> {
        self.records
            .iter()
            .filter(|s| s.coltype == "table")
            .map(|s| s.name.to_string())
            .collect()
    }
}

impl TryFrom<Vec<Record>> for SchemaTable {
    type Error = anyhow::Error;

    fn try_from(records: Vec<Record>) -> anyhow::Result<Self> {
        let schema_records = records
            .into_iter()
            // we only keep the valid records
            .filter_map(|r| SchemaTableRecord::try_from(r).ok())
            .collect::<Vec<_>>();

        Ok(Self {
            records: schema_records,
        })
    }
}
#[derive(Debug)]
pub struct SchemaTableRecord {
    pub coltype: String,
    pub name: String,
    pub tbl_name: String,
    pub rootpage: u64,
    pub sql: String,
}

impl TryFrom<Record> for SchemaTableRecord {
    type Error = anyhow::Error;

    fn try_from(record: Record) -> anyhow::Result<Self> {
        dbg!(&record);
        if record.column_contents.len() != 5 {
            anyhow::bail!("Wrong number of columns to build the schema table");
        }

        let coltype = match &record.column_contents[0] {
            ColumnContent::String(x) => x.to_string(),
            _ => anyhow::bail!("Wrong column type for schema table"),
        };
        let name = match &record.column_contents[1] {
            ColumnContent::String(x) => x.to_string(),
            _ => anyhow::bail!("Wrong column type for schema table"),
        };
        let tbl_name = match &record.column_contents[2] {
            ColumnContent::String(x) => x.to_string(),
            _ => anyhow::bail!("Wrong column type for schema table"),
        };
        let rootpage = match &record.column_contents[3] {
            ColumnContent::Int(x) => *x,
            _ => anyhow::bail!("Wrong column type for schema table"),
        };
        let sql = match &record.column_contents[4] {
            ColumnContent::String(x) => x.to_string(),
            _ => anyhow::bail!("Wrong column type for schema table"),
        };

        Ok(SchemaTableRecord {
            coltype,
            name,
            tbl_name,
            rootpage,
            sql,
        })
    }
}
