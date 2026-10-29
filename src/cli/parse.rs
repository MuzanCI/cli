use std::path::PathBuf;

use muzanci_config::collector::Env;
use muzanci_git::GitClient;
use muzanci_observability::TError;

pub fn parse_key_val(s: &str) -> Result<(String, String), String> {
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=VALUE: no `=` found in [{s}]"))?;
    let key = s[..pos].trim();
    let value = s[pos + 1..].trim();
    if key.is_empty() {
        let e = format!("invalid KEY=VALUE: key is empty in [{s}]");
        return Err(e);
    }
    Ok((key.to_string(), value.to_string()))
}

pub fn parse_env(env: Vec<(String, String)>) -> Result<Env, TError> {
    let mut env = env
        .into_iter()
        .try_fold(Env::new(), |mut acc, (key, value)| {
            if acc.contains_key(&key) {
                return Err(TError::new(&format!("Duplicate env key [{key}]")));
            }
            acc.insert(key, value);
            Ok(acc)
        })?;

    if !env.contains_key("GIT_BRANCH") {
        let branch = GitClient::try_default()?.get_branch(&PathBuf::from("."))?;
        env.insert("GIT_BRANCH".to_string(), branch);
    }

    if !env.contains_key("GIT_COMMIT") {
        let commit = GitClient::try_default()?.get_commit(&PathBuf::from("."))?;
        env.insert("GIT_COMMIT".to_string(), commit);
    }

    Ok(env)
}
