use prettytable::{Table, Row, Cell};

use anyhow::Context;
use crate::server::detect;
use crate::server::linux;
use crate::server::macos;
use crate::server::options::Info;
use crate::server::package::Package;
use crate::server::version::{Version, VersionQuery, VersionMarker};
use crate::table;


#[derive(serde::Serialize)]
#[serde(rename_all="kebab-case")]
struct JsonInfo<'a> {
    installation_method: &'a str,
    major_version: &'a VersionMarker,
    version: &'a Version<String>,
    binary_path: Option<&'a str>,
}

pub fn info(options: &Info) -> anyhow::Result<()> {
    let current_os = detect::current_os()?;

    let filter = options.latest || options.nightly
        || options.version.is_some() || options.method.is_some();
    if !filter {
        log::warn!("`server info` without filter is deprecated. \
                    Assuming --latest");
    }
    let version_query = VersionQuery::new(
        options.nightly, options.version.as_ref());
    let method = if let Some(meth) = &options.method {
        current_os.single_method(meth)?
    } else {
        current_os.first_method()?
    };
    let distr = method.installed_versions()?.into_iter()
        .filter(|p| version_query.matches(p.version_slot()))
        .max_by(|a, b| a.version_slot().cmp(b.version_slot()))
        .with_context(|| format!("cannot find distribution for your \
                                  criteria. Try `edgedb server install` \
                                  with the same parameters"))?;

    let cmd = distr.downcast_ref::<Package>().map(|pkg| {
        if cfg!(target_os="macos") {
            macos::get_server_path(pkg.slot.slot_name())
        } else {
            linux::get_server_path(Some(pkg.slot.slot_name()))
        }
    });
    if options.bin_path {
        if let Some(cmd) = cmd {
            if options.json {
                if let Some(cmd) = cmd.to_str() {
                    println!("{}", serde_json::to_string(cmd)?);
                } else {
                    anyhow::bail!("Path {:?} can't be represented as JSON",
                        cmd);
                }
            } else {
                println!("{}", cmd.display());
            }
        } else {
            anyhow::bail!("cannot print binary path for {} installation",
                method.name().option());
        }
    } else if options.json {
        println!("{}", serde_json::to_string_pretty(&JsonInfo {
            installation_method: method.name().short_name(),
            major_version: &distr.version_slot().to_marker(),
            version: distr.version(),
            binary_path: cmd.as_ref().and_then(|cmd| cmd.to_str()),
        })?)
    } else {
        let mut table = Table::new();
        table.add_row(Row::new(vec![
            Cell::new("Installation method"),
            Cell::new(method.name().title()),
        ]));
        table.add_row(Row::new(vec![
            Cell::new("Major version"),
            Cell::new(&distr.version_slot().title().to_string()),
        ]));
        table.add_row(Row::new(vec![
            Cell::new("Exact version"),
            Cell::new(distr.version().as_ref()),
        ]));
        if let Some(cmd) = cmd {
            table.add_row(Row::new(vec![
                Cell::new("Binary path"),
                Cell::new(&cmd.display().to_string()),
            ]));
        }
        table.set_format(*table::FORMAT);
        table.printstd();
    }
    Ok(())
}
