// hephaestus-core/src/common.rs

/// Toutes les colonnes techniques possibles du pipeline.
/// Chaque stratégie n'utilise que le sous-ensemble qui la concerne.
pub struct TechnicalColumns {
    // Append
    pub ingested_at: String,
    pub pk_hash: String,
    // SCD2 / SCD3
    pub valid_from: String,
    pub valid_to: String,
    pub is_current: String,
}

impl Default for TechnicalColumns {
    fn default() -> Self {
        Self {
            ingested_at: "_ingested_at".into(),
            pk_hash: "_pk_hash".into(),
            valid_from: "valid_from".into(),
            valid_to: "valid_to".into(),
            is_current: "is_current".into(),
        }
    }
}

/// Sous-ensemble utilisé par la stratégie append.
pub struct AppendColumns {
    pub ingested_at: String,
    pub pk_hash: String,
}

impl From<&TechnicalColumns> for AppendColumns {
    fn from(t: &TechnicalColumns) -> Self {
        Self {
            ingested_at: t.ingested_at.clone(),
            pk_hash: t.pk_hash.clone(),
        }
    }
}