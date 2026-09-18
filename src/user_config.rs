use std::io::Read;
use std::path::Path;

use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::prompt::Intent;
use crate::prompt::prompt_account_selection;
use crate::prompt::prompt_login;

#[derive(Serialize, Deserialize)]
pub struct UserConfig {
    pub personal_access_token_secret: Option<String>,
    pub account_id: Option<Uuid>,
}

impl UserConfig {
    pub fn from_file(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config = toml::from_str(&contents)?;
        Ok(config)
    }

    pub fn to_file(&self, path: &Path) -> anyhow::Result<()> {
        let contents = toml::to_string(self)?;
        std::fs::write(path, contents)?;
        Ok(())
    }
}
