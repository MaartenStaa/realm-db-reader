use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use log::warn;

use realm_rust::group::Group;
use realm_rust::realm::Realm;
use realm_rust::table::Row;
use realm_rust::value::{Backlink, Value};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Debug,
    Parse,
    Stress,
    Test,
}

fn main() -> anyhow::Result<()> {
    pretty_env_logger::init();

    let filename = PathBuf::from(
        "/Users/maartens/Downloads/LukiMain_d98d3d86-49d4-50fe-9f6a-77d25e72075e_46C4F83D-6ABB-408B-9F5D-EC1829AF1FDD.realm",
    );

    let cli = Cli::parse();
    let realm = Realm::open(filename)?;

    match cli.command {
        Command::Debug => {
            realm.walk_tree()?;
        }
        Command::Parse => {
            let mut group = Group::build(realm.into_top_ref_array()?)?;
            println!("{group:#?}");

            // let folder_id = "C4F2AFBC-F3A0-4758-BC0E-12D136BEAAFE";
            // let folders = group.get_table_by_name_mut("class_FolderDataModel")?;
            //
            // let folder =
            //     folders.find_row_from_indexed_column("id", &Value::String(folder_id.to_owned()));
            // dbg!(&folder);
            //
            // return Ok(());
            //
            // let block_id = "6A3DC4E2-1430-49F8-8244-4EEE26EE8ADF";
            //
            for i in 0..group.table_count() {
                let name = group.get_table_name(i).to_owned();
                eprintln!("Table {i}: {name}");

                let table = match group.get_table_mut(i) {
                    Ok(table) => table,
                    Err(e) => {
                        log::error!("Failed to get table {i}: {name}");
                        log::error!("Error: {e}");

                        continue;
                    }
                };

                log::warn!("{table:?}");

                let row_count = match table.row_count() {
                    Ok(n) => {
                        eprintln!("Table {name} has {n} rows");
                        n
                    }
                    Err(e) => {
                        log::error!("Failed to get row count for table {i}: {name}");
                        log::error!("Error: {e}");
                        continue;
                    }
                };

                for r in 0..(row_count.min(10)) {
                    match table.get_row(r) {
                        Ok(row) => {
                            eprintln!("Row {r} in table {i}: {row:?}");
                        }
                        Err(e) => {
                            log::error!("Failed to get row {r} for table {i}: {name}");
                            log::error!("Error: {e}");
                            continue;
                        }
                    }
                }
            }

            return Ok(());

            let pk = group.get_table_by_name_mut("pk")?;
            dbg!(&pk);
            let row = pk.get_row(0)?;
            dbg!(&row);

            let block_id = "F94489F7-25C8-4190-9487-F4FCD3387F6F";
            let blocks = group.get_table_by_name_mut("class_BlockDataModel")?;

            let mut path = Vec::new();

            let mut block =
                blocks.find_row_from_indexed_column("id", &Value::String(block_id.to_owned()))?;
            let mut document_id = None;
            dbg!(&block);
            // dbg!(&blocks);

            while let Some(block_) = block {
                if let Value::String(s) = block_.get("content").unwrap() {
                    path.push(s.clone());
                }
                if let Value::String(s) = block_.get("documentId").unwrap() {
                    document_id = Some(s.clone());
                }

                let Some(b) = block_.backlinks().next() else {
                    warn!("no backlink found");
                    break;
                };

                dbg!(&b);

                let other_table = group.get_table_mut(b.origin_table_index)?;

                block = Some(other_table.get_row(b.row_indexes[0])?);
                dbg!(&block);
            }

            dbg!(&path);

            if let Some(document_id) = document_id {
                // documentId = column index 18
                let documents = group.get_table_by_name_mut("class_DocumentDataModel")?;
                let document =
                    documents.find_row_from_indexed_column("id", &Value::String(document_id))?;

                dbg!(&document);

                if let Some(document) = document {
                    let Some(document_backlink) = document.backlinks().next() else {
                        warn!("last value is not a backlink");
                        return Ok(());
                    };

                    let folders = group.get_table_mut(document_backlink.origin_table_index)?;
                    let mut folder = Some(folders.get_row(document_backlink.row_indexes[0])?);

                    while let Some(folder_) = folder {
                        dbg!(&folder_);

                        if let Value::String(name) = folder_.get("name").unwrap() {
                            path.push(name.clone());
                        }

                        if let Some(Value::String(parent_id)) =
                            folder_.get("parentFolderId").cloned()
                        {
                            dbg!(&parent_id);
                            folder = folders
                                .find_row_from_indexed_column("id", &Value::String(parent_id))?;
                        } else {
                            warn!("No parent found for folder");
                            break;
                        }
                    }

                    let all_folders = folders.get_rows()?;
                    dbg!(&all_folders);
                }
            } else {
                warn!("No document ID found in the blocks");
            }

            dbg!(&path);

            return Ok(());

            warn!("about to go find the table by name");
            let folders = group.get_table_by_name_mut("class_FolderDataModel")?;
            // let folders = group.get_table_by_name_mut("pk")?;
            dbg!(&folders);
            // if let Some(Value::Table(table)) = row.get_mut(row.len() - 1) {
            //     let sub = table.get_rows()?;
            //     dbg!(&sub);
            // }
            dbg!(folders.get_rows()?);
            // let rows = folders.get_rows()?;
            // dbg!(&rows);
        }
        Command::Stress => {
            // Try to just load everything in the realm
            let mut group = Group::build(realm.into_top_ref_array()?)?;
            for table_index in 0..group.table_count() {
                let name = group.get_table_name(table_index).to_owned();
                let mut indexed_column_names: Vec<&str> = vec![];
                let mut indexed_values: HashMap<String, Vec<(Value, usize)>> = HashMap::new();

                log::info!("Loading table {table_index}: {name}");

                let table = group.get_table_mut(table_index)?;

                // for column in table.get_column_specs().iter() {
                //     if column.get_attributes().is_indexed() {
                //         if let ColumnSpec::Regular { name, .. } = column {
                //             indexed_column_names.push(name.to_owned());
                //         }
                //     }
                // }

                let rows = table.get_rows()?;
                println!("Table {} contains {} rows", name, rows.len());

                for indexed_column_name in &indexed_column_names {
                    indexed_values.insert(
                        indexed_column_name.to_string(),
                        rows.iter()
                            .enumerate()
                            .filter_map(|(row_index, row)| {
                                row.get(indexed_column_name)
                                    .filter(|value| match value {
                                        Value::None => false,
                                        Value::String(s) if s.is_empty() => false,
                                        _ => true,
                                    })
                                    .map(|value| (value.clone(), row_index))
                            })
                            .collect(),
                    );
                }

                // Ensure we can find every value in its index
                for (column_name, indexed_values) in indexed_values {
                    for (value, row_index) in indexed_values {
                        let Some(found_row_index) =
                            table.find_row_index_from_indexed_column(&column_name, &value)?
                        else {
                            log::error!(
                                "Value {:?} in column {} at row {} not found in index",
                                value,
                                column_name,
                                row_index
                            );

                            continue;
                        };

                        if found_row_index != row_index {
                            log::error!(
                                "Value {:?} in column {} at row {} not found at expected index {}",
                                value,
                                column_name,
                                row_index,
                                found_row_index
                            );
                        }
                    }
                }
            }
        }
        Command::Test => {
            let mut group = Group::build(realm.into_top_ref_array()?)?;
            // let table = group.get_table_by_name_mut("class_BlockDataModel")?;
            // let value = Value::Timestamp(DateTime::from_timestamp(2147483647, 63999891).unwrap());
            //
            // let row = table.find_row_from_indexed_column("updated", &value)?;
            let table = group.get_table_mut(2)?;
            let row = table.get_row(0)?;

            dbg!(&row);
        }
    }

    // if args().count() == 3 {
    //     realm.walk(realm.hdr.current_top_ref(), 0, None)?;
    //     realm.walk_tree()?;
    // } else {
    //     // let mut deserializer = RealmDeserializer::new(realm.top_ref());
    //     let mut deserializer = RealmNode::from_offset(&realm, realm.hdr.current_top_ref())?;
    //     eprintln!(
    //         "Deserializing top_ref at offset: 0x{:X}",
    //         realm.hdr.current_top_ref()
    //     );
    //     let top_ref: TopRef = deserializer.deserialize()?;
    //     dbg!(&top_ref);
    // }
    //

    Ok(())
}
