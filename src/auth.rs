use crate::config::App;

pub fn check_api_key(apps: &[App], api_key: &str) -> Option<String> {
    apps.iter()
        .find(|app| app.api_key == api_key)
        .map(|app| app.app_id.clone())
}
