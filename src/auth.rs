use crate::config::Tenant;

pub fn check_api_key(tenants: &[Tenant], api_key: &str) -> Option<String> {
    tenants
        .iter()
        .find(|tenant| tenant.api_key == api_key)
        .map(|tenant| tenant.tenant_id.clone())
}
