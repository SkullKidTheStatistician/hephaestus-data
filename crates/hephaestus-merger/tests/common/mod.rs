#![allow(dead_code)]

use hephaestus_merger::common::AppendColumns;
use polars::prelude::*;

pub fn cols() -> AppendColumns {
    AppendColumns {
        ingested_at: "_ingested_at".into(),
        pk_hash: "_pk_hash".into(),
    }
}

pub fn pk_hashes(df: &DataFrame) -> Vec<u64> {
    df.column("_pk_hash")
        .unwrap()
        .u64()
        .unwrap()
        .iter()
        .map(|v| v.unwrap())
        .collect()
}