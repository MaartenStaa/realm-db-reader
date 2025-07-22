use std::path::PathBuf;

use clap::{Parser, Subcommand};
use log::warn;

use crate::build::Build;
use crate::group::Group;
use crate::realm::Realm;
use crate::value::Value;

mod array;
mod build;
mod column;
mod debug;
mod group;
mod index;
mod node;
mod realm;
mod spec;
mod table;
mod utils;
mod value;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Debug,
    Parse,
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

            let block_id = "6A3DC4E2-1430-49F8-8244-4EEE26EE8ADF";
            // let document_id = ""
            let blocks = group.get_table_by_name_mut("class_BlockDataModel")?;
            dbg!(&blocks);

            let mut block =
                blocks.find_row_from_index("id", &Value::String(block_id.to_owned()))?;
            dbg!(&block);
            // dbg!(&blocks);

            loop {
                if let Some(block_) = block {
                    let Value::BackLink(b) = block_[block_.len() - 1].clone() else {
                        warn!("last value is not a backlink");
                        return Ok(());
                    };

                    dbg!(&b);

                    let other_table = group.get_table_mut(b.origin_table_index)?;
                    // dbg!(other_table.get_column_spec(b.origin_column_index));
                    // dbg!(&other_table);

                    block = Some(other_table.get_row(b.row_index)?);
                    dbg!(&block);
                } else {
                    break;
                }
            }

            return Ok(());

            warn!("about to go find the table by name");
            let folders = group.get_table_by_name_mut("class_FolderDataModel")?;
            // let folders = group.get_table_by_name_mut("pk")?;
            dbg!(&folders);
            warn!("about to go fetch row 0");
            let row = folders.get_row_mut(0)?;
            dbg!(&row);
            // if let Some(Value::Table(table)) = row.get_mut(row.len() - 1) {
            //     let sub = table.get_rows()?;
            //     dbg!(&sub);
            // }
            dbg!(folders.get_rows()?);
            // let rows = folders.get_rows()?;
            // dbg!(&rows);
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
