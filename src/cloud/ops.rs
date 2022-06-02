use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use async_std::task;
use edgedb_client::credentials::Credentials;
use edgedb_client::Builder;

use crate::cloud::client::CloudClient;
use crate::credentials;
use crate::options::CloudOptions;
use crate::print::{self, Highlight};
use crate::table::{self, Cell, Row, Table};

const INSTANCE_CREATION_WAIT_TIME: Duration = Duration::from_secs(5 * 60);
const INSTANCE_CREATION_POLLING_INTERVAL : Duration = Duration::from_secs(1);

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CloudInstance {
    id: String,
    name: String,
    pub dsn: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls_ca: Option<String>,
    org_slug: String,
}

impl CloudInstance {
    pub fn as_credentials(&self) -> anyhow::Result<Credentials> {
        let mut creds = task::block_on(Builder::uninitialized()
            .read_dsn(&self.dsn))?
            .as_credentials()?;
        creds.tls_ca = self.tls_ca.clone();
        creds.cloud_instance_id = Some(self.id.clone());
        creds.cloud_original_dsn = Some(self.dsn.clone());
        Ok(creds)
    }
}

#[derive(Debug, serde::Serialize)]
struct InstanceStatus {
    cloud_instance: CloudInstance,
    credentials: Option<Credentials>,
    instance_name: Option<String>,
}

impl InstanceStatus {
    fn from_cloud_instance(cloud_instance: CloudInstance) -> Self {
        Self {
            cloud_instance,
            credentials: None,
            instance_name: None,
        }
    }

    fn print_extended(&self) {
        println!("{}:", self.cloud_instance.name);

        println!("  Status: {}", self.cloud_instance.status);
        println!("  ID: {}", self.cloud_instance.id);
        if let Some(name) = &self.instance_name {
            println!("  Local Instance: {}", name);
        }
        if let Some(creds) = &self.credentials {
            if let Some(dsn) = &creds.cloud_original_dsn {
                println!("  DSN: {}", dsn);
            }
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct Org {
    pub id: String,
    pub name: String,
    pub slug: String,
}

#[derive(Debug, serde::Serialize)]
pub struct CloudInstanceCreate {
    pub name: String,
    pub org_slug: String,
    // #[serde(skip_serializing_if = "Option::is_none")]
    // pub version: Option<String>,
    // #[serde(skip_serializing_if = "Option::is_none")]
    // pub default_database: Option<String>,
    // #[serde(skip_serializing_if = "Option::is_none")]
    // pub default_user: Option<String>,
}

pub async fn find_cloud_instance_by_name(
    org_slug: &str,
    name: &str,
    client: &CloudClient,
) -> anyhow::Result<CloudInstance> {
    // todo: does making this return an error instead of None break anything?
    let instance: CloudInstance = client.get(format!("orgs/{}/instances/{}", org_slug, name)).await?;
    Ok(instance)
}

async fn wait_instance_create(
    mut instance: CloudInstance,
    client: &CloudClient,
    quiet: bool,
) -> anyhow::Result<CloudInstance> {
    if !quiet && instance.status == "creating" {
        print::echo!("Waiting for EdgeDB Cloud instance creation...");
    }
    let url = format!("orgs/{}/instances/{}", instance.org_slug, instance.name);
    let deadline = Instant::now() + INSTANCE_CREATION_WAIT_TIME;
    while Instant::now() < deadline {
        if instance.dsn != "" {
            return Ok(instance);
        }
        if instance.status != "available" && instance.status != "creating" {
            anyhow::bail!(
                "Failed to create EdgeDB Cloud instance: {}",
                instance.status
            );
        }
        if instance.status == "creating" {
            task::sleep(INSTANCE_CREATION_POLLING_INTERVAL).await;
        }
        instance = client.get(&url).await?;
    }
    if instance.dsn != "" {
        Ok(instance)
    } else {
        anyhow::bail!("Timed out.")
    }
}

async fn write_credentials(cred_path: &PathBuf, instance: CloudInstance) -> anyhow::Result<()> {
    let mut creds = Builder::uninitialized()
        .read_dsn(&instance.dsn)
        .await?
        .as_credentials()?;
    creds.tls_ca = instance.tls_ca;
    creds.cloud_instance_id = Some(instance.id);
    creds.cloud_original_dsn = Some(instance.dsn);
    credentials::write(&cred_path, &creds).await
}

pub async fn create_cloud_instance(
    client: &CloudClient,
    instance: &CloudInstanceCreate,
) -> anyhow::Result<()> {
    let url = format!("orgs/{}/instances", instance.org_slug);
    let instance: CloudInstance = client
        .post(url, serde_json::to_value(instance)?)
        .await?;
    wait_instance_create(instance, client, false).await?;
    Ok(())
}

pub fn split_cloud_instance_name(name: &str) -> anyhow::Result<(String, String)> {
    let mut splitter = name.splitn(2, '/');
    match splitter.next() {
        None => anyhow::bail!("unreachable"),
        Some("") => anyhow::bail!("empty instance name"),
        Some(org) => match splitter.next() {
            None => anyhow::bail!("cloud instance must be in the form ORG/INST"),
            Some("") => anyhow::bail!("invalid instance name: missing instance"),
            Some(inst) => Ok((String::from(org), String::from(inst))),
        },
    }
}

pub async fn create(
    cmd: &crate::portable::options::Create,
    opts: &crate::options::Options,
) -> anyhow::Result<()> {
    let client = CloudClient::new(&opts.cloud_options)?;
    client.ensure_authenticated(false)?;

    let (org_slug, inst_name) = split_cloud_instance_name(&cmd.name)?;
    let instance = CloudInstanceCreate {
        name: inst_name,
        org_slug,
        // version: Some(format!("{}", version.display())),
        // default_database: Some(cmd.default_database.clone()),
        // default_user: Some(cmd.default_user.clone()),
    };
    // todo check for 404 and print message about org not existing
    create_cloud_instance(&client, &instance).await?;
    print::echo!(
        "EdgeDB Cloud instance",
        cmd.name.emphasize(),
        "is up and running."
    );
    print::echo!("To connect to the instance run:");
    print::echo!("  edgedb -I", cmd.name);
    Ok(())
}

#[derive(Debug, serde::Deserialize)]
pub struct InstanceName {
    pub instance_name: String,
    pub org_slug: String,
}

async fn destroy(name: &str, org_slug: &str, options: &CloudOptions) -> anyhow::Result<()> {
    log::info!("Destroying EdgeDB Cloud instance: {}/{}", org_slug, name);
    let client = CloudClient::new(options)?;
    client.ensure_authenticated(false)?;
    let _: CloudInstance = client.delete(format!("orgs/{}/instances/{}", org_slug, name)).await?;
    Ok(())
}

pub fn try_to_destroy(
    name: &str,
    org_slug: &str,
    options: &crate::options::Options,
) -> anyhow::Result<()> {
    task::block_on(destroy(name, org_slug, &options.cloud_options))?;
    Ok(())
}

pub async fn list(
    cmd: &crate::portable::options::List,
    opts: &crate::options::Options,
) -> anyhow::Result<()> {
    let client = CloudClient::new(&opts.cloud_options)?;
    client.ensure_authenticated(false)?;
    let cloud_instances: Vec<CloudInstance> = client.get("instances/").await?;
    let mut instances = cloud_instances
        .into_iter()
        .map(|inst| (inst.id.clone(), InstanceStatus::from_cloud_instance(inst)))
        .collect::<HashMap<String, InstanceStatus>>();
    for name in credentials::all_instance_names()? {
        let file = io::BufReader::new(fs::File::open(credentials::path(&name)?)?);
        let creds: Credentials = serde_json::from_reader(file)?;
        if let Some(id) = &creds.cloud_instance_id {
            if let Some(instance) = instances.get_mut(id) {
                (*instance).instance_name = Some(name);
                (*instance).credentials = Some(creds);
            }
        }
    }
    if instances.is_empty() {
        if cmd.json {
            println!("[]");
        } else if !cmd.quiet {
            print::warn("No instances found");
        }
        return Ok(());
    }
    if cmd.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&instances.into_values().collect::<Vec<_>>())?
        );
    } else if cmd.debug {
        for instance in instances.values() {
            println!("{:#?}", instance);
        }
    } else if cmd.extended {
        for instance in instances.values() {
            instance.print_extended();
        }
    } else {
        let mut table = Table::new();
        table.set_format(*table::FORMAT);
        table.set_titles(Row::new(
            ["Kind", "Name", "Status"]
                .iter()
                .map(|x| table::header_cell(x))
                .collect(),
        ));
        for instance in instances.values() {
            table.add_row(Row::new(vec![
                Cell::new("cloud"),
                Cell::new(&format!("{}/{}", &instance.cloud_instance.org_slug, &instance.cloud_instance.name)),
                Cell::new(&instance.cloud_instance.status),
            ]));
        }
        table.printstd();
    }
    Ok(())
}

pub async fn link_existing_cloud_instance(client: &CloudClient, org: &str, name: &str) -> anyhow::Result<()> {
    let cred_path = credentials::path(&name)?;
    if cred_path.exists() {
        // todo: is this reachable?
        anyhow::bail!(
            "{:?} is a locally-used name, try --link without --cloud",
            name,
        );
    }
    let inst = find_cloud_instance_by_name(org, name, client).await?;
    let inst = wait_instance_create(inst, client, false).await?;
    write_credentials(&cred_path, inst).await?;
    Ok(())
}
