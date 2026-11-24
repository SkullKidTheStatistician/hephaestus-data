// crates/hephaestus-merger/tests/append_validation.rs
mod common;

use common::{cols, pk_hashes};
use hephaestus_merger::append::append;
use polars::df;
use polars::prelude::*;

fn pk(names: &[&str]) -> Vec<String> {
    names.iter().map(|s| s.to_string()).collect()
}

#[test]
fn successive_appends_accumulate_rows() {
    // Batch 1 : pas de table existante
    let batch1 = df!(
        "order_id" => &[1i64, 2],
        "amount"   => &[10.0f64, 20.0],
    )
    .unwrap()
    .lazy();
    let table = append(batch1, None, &pk(&["order_id"]), &cols())
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(table.height(), 2);

    // Batch 2 : la table du batch 1 devient "existing"
    let batch2 = df!(
        "order_id" => &[3i64],
        "amount"   => &[30.0f64],
    )
    .unwrap()
    .lazy();
    let table = append(batch2, Some(table.lazy()), &pk(&["order_id"]), &cols())
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(table.height(), 3);

    // Batch 3
    let batch3 = df!(
        "order_id" => &[4i64, 5],
        "amount"   => &[40.0f64, 50.0],
    )
    .unwrap()
    .lazy();
    let table = append(batch3, Some(table.lazy()), &pk(&["order_id"]), &cols())
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(table.height(), 5);
    assert_eq!(table.column("_pk_hash").unwrap().null_count(), 0);
    assert_eq!(table.column("_ingested_at").unwrap().null_count(), 0);
}

#[test]
fn same_key_gets_same_hash_across_batches() {
    let batch1 = df!("order_id" => &[42i64], "amount" => &[1.0f64])
        .unwrap()
        .lazy();
    let table = append(batch1, None, &pk(&["order_id"]), &cols())
        .unwrap()
        .collect()
        .unwrap();

    // Même clé dans un 2e batch (append pur : pas de dédup, les 2 lignes restent)
    let batch2 = df!("order_id" => &[42i64], "amount" => &[2.0f64])
        .unwrap()
        .lazy();
    let table = append(batch2, Some(table.lazy()), &pk(&["order_id"]), &cols())
        .unwrap()
        .collect()
        .unwrap();

    let h = pk_hashes(&table);
    assert_eq!(h.len(), 2);
    assert_eq!(h[0], h[1], "même clé => même hash, d'un batch à l'autre");
}

#[test]
fn composite_pk_with_nulls_across_batches() {
    let batch1 = df!(
        "k" => &["a", "a"],
        "v" => &[Some(""), None],
    )
    .unwrap()
    .lazy();
    let table = append(batch1, None, &pk(&["k", "v"]), &cols())
        .unwrap()
        .collect()
        .unwrap();

    let batch2 = df!(
        "k" => &["a"],
        "v" => &[None::<&str>],
    )
    .unwrap()
    .lazy();
    let table = append(batch2, Some(table.lazy()), &pk(&["k", "v"]), &cols())
        .unwrap()
        .collect()
        .unwrap();

    let h = pk_hashes(&table);
    assert_eq!(h.len(), 3);
    assert_ne!(h[0], h[1], r#"("a","") doit différer de ("a", NULL)"#);
    assert_eq!(h[1], h[2], r#"("a", NULL) doit rester stable entre batches"#);
}

#[test]
fn missing_pk_column_is_rejected() {
    let batch = df!("amount" => &[10.0f64]).unwrap().lazy();
    let res = append(batch, None, &pk(&["order_id"]), &cols());
    assert!(res.is_err());
}