use std::{env, fs};

use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub struct ModelConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
}

#[derive(Clone, Deserialize)]
pub struct App {
    pub api_key: String,
    pub app_id: String,
    pub llm: ModelConfig,
    #[allow(dead_code)]
    pub name: String,
}

pub fn load_apps() -> Result<Vec<App>, String> {
    let path = env::var("CONFIG_FILE").unwrap_or_else(|_| "config.json".into());
    let raw = fs::read_to_string(&path).map_err(|e| format!("read {path}: {e}"))?;
    let apps: Vec<App> = serde_json::from_str(&raw).map_err(|e| format!("parse {path}: {e}"))?;
    for app in &apps {
        if app.api_key.trim().is_empty()
            || app.app_id.trim().is_empty()
            || app.llm.api_key.trim().is_empty()
            || app.llm.model.trim().is_empty()
            || !(app.llm.base_url.starts_with("https://")
                || app.llm.base_url.starts_with("http://"))
        {
            return Err(format!(
                "app {} must configure api_key, app_id, and llm.base_url/api_key/model",
                app.app_id
            ));
        }
    }
    Ok(apps)
}

#[cfg(test)]
mod tests {
    use super::App;

    #[test]
    fn default_config_example_matches_the_app_schema() {
        let apps: Vec<App> = serde_json::from_str(include_str!("../default_config.json")).unwrap();
        assert!(!apps.is_empty());
        assert!(apps.iter().all(|app| !app.app_id.is_empty()));
    }
}
