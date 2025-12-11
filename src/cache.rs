use crate::Url;
use serde::{Deserialize, Serialize};
use std::io::ErrorKind;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug)]
pub struct Model {
    pub brand: String,
    pub model: String,
    pub url: String,
}

pub fn read_models(path: &str) -> Result<Vec<Model>, anyhow::Error> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(vec![]),
        Err(err) => return Err(err.into()),
    };
    match file
        .metadata()?
        .modified()?
        .elapsed()
        .map(|e| e >= Duration::from_secs(60 * 60 * 24))
    {
        Ok(true) => return Ok(vec![]),
        Ok(false) => (),
        Err(err) => {
            log::error!("{err}");
            return Ok(vec![]);
        }
    }
    let input = std::fs::read_to_string(path)?;
    let models = serde_yaml::from_str(&input)?;
    Ok(models)
}

pub fn write_models(path: &str, models: Vec<Model>) -> Result<(), anyhow::Error> {
    let models = serde_yaml::to_string(&models)?;
    std::fs::write(path, models)?;
    Ok(())
}

pub fn write_links(path: &str, links: &[(Url, String, String)]) -> Result<(), anyhow::Error> {
    let links = serde_yaml::to_string(
        &links
            .iter()
            .map(|(Url(url), model, brand)| (url, model, brand))
            .collect::<Vec<_>>(),
    )?;
    std::fs::write(path, links)?;
    Ok(())
}

pub fn read_links(path: &str) -> Result<Vec<(Url, String, String)>, anyhow::Error> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(vec![]),
        Err(err) => return Err(err.into()),
    };
    match file
        .metadata()?
        .modified()?
        .elapsed()
        .map(|e| e >= Duration::from_secs(60 * 60 * 24))
    {
        Ok(true) => return Ok(vec![]),
        Ok(false) => (),
        Err(err) => {
            log::error!("{err}");
            return Ok(vec![]);
        }
    }
    let input = std::fs::read_to_string(path)?;
    let models: Vec<(String, String, String)> = serde_yaml::from_str(&input)?;
    Ok(models
        .into_iter()
        .map(|(url, model, brand)| (Url(url), model, brand))
        .collect())
}

pub fn clean_links(path: &str) -> Result<(), anyhow::Error> {
    Ok(std::fs::remove_file(path)?)
}
