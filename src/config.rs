use std::{env, fs};

use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub struct ModelConfig {
    pub base_url: String,
    pub api_key: String,
    pub model: String,
}

#[derive(Clone, Deserialize)]
pub struct Tenant {
    pub api_key: String,
    pub tenant_id: String,
    pub llm: ModelConfig,
    #[allow(dead_code)]
    pub name: String,
}

pub fn load_tenants() -> Result<Vec<Tenant>, String> {
    let path = env::var("TENANTS_FILE").unwrap_or_else(|_| "tenants.json".into());
    let raw = fs::read_to_string(&path).map_err(|e| format!("read {path}: {e}"))?;
    let tenants: Vec<Tenant> =
        serde_json::from_str(&raw).map_err(|e| format!("parse {path}: {e}"))?;
    for tenant in &tenants {
        if tenant.api_key.trim().is_empty()
            || tenant.tenant_id.trim().is_empty()
            || tenant.llm.api_key.trim().is_empty()
            || tenant.llm.model.trim().is_empty()
            || !(tenant.llm.base_url.starts_with("https://")
                || tenant.llm.base_url.starts_with("http://"))
        {
            return Err(format!(
                "tenant {} must configure api_key, tenant_id, and llm.base_url/api_key/model",
                tenant.tenant_id
            ));
        }
    }
    Ok(tenants)
}
