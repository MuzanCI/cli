use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

use directories::ProjectDirs;
use muzanci_observability::TError;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

pub struct UserConfig {
    path: PathBuf,
    fields: UserConfigFields,
}

#[derive(Serialize, Deserialize)]
pub struct UserConfigFields {
    pub personal_access_token_secret: Option<String>,
    pub account_id: Option<Uuid>,
    pub disable_telemetry: Option<bool>,
}

impl UserConfig {
    pub fn from_file(path: &Path) -> Result<Self, TError> {
        let fields = {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .truncate(false)
                .open(path)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            toml::from_str(&contents)?
        };

        let config = Self {
            path: path.to_path_buf(),
            fields,
        };

        Ok(config)
    }

    pub fn fields(&self) -> &UserConfigFields {
        &self.fields
    }

    pub fn fields_mut(&mut self) -> &mut UserConfigFields {
        &mut self.fields
    }

    pub fn save(&self) -> Result<(), TError> {
        let contents = toml::to_string(&self.fields)?;
        std::fs::write(&self.path, contents)?;
        Ok(())
    }

    pub fn default_path() -> PathBuf {
        if let Some(proj_dirs) = ProjectDirs::from("com", "muzanci", "muzan-cli") {
            // Linux/FreeBSD: ~/.config/muzan-cli/config.toml
            // macOS:         ~/Library/Application Support/com.muzanci.muzan-cli/config.toml
            // Windows:       %USERPROFILE%\AppData\Roaming\muzanci\muzan-cli\config\config.toml
            proj_dirs.config_dir().join("config.toml")
        } else {
            // Fallback to current directory if home directory cannot be resolved
            PathBuf::from("config.toml")
        }
    }
}
