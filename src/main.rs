use std::path::PathBuf;

use clap::{Parser, Subcommand};
use log::warn;

use crate::build::Build;
use crate::group::Group;
use crate::realm::Realm;

mod array;
mod build;
mod column;
mod debug;
mod group;
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
        "/Users/maartens/Downloads/LukiMain_d98d3d86-49d4-50fe-9f6a-77d25e72075e_46C4F83D-6ABB-408B-9F5D-EC1829AF1FDD 2.realm",
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

            warn!("about to go find the table by name");
            let folders = group.get_table_by_name_mut("class_FolderDataModel")?;
            dbg!(&folders);
            warn!("about to go fetch row 0");
            let row = folders.get_row(0)?;
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
