use polars::prelude::*;
use std::path::Path;

/// Convertit un `&Path` en `PlRefPath` (seul endroit où cette conversion existe).
pub(crate) fn to_pl_path(path: &Path) -> PolarsResult<PlRefPath> {
    let s = path.to_str().ok_or_else(|| {
        polars_err!(ComputeError: "Non-UTF8 path: {}", path.display())
    })?;
    Ok(PlRefPath::new(s))
}

/// Lit une table Parquet (fichier ou dossier) en LazyFrame.
///
/// Si le chemin n'existe pas :
/// - avec un schéma  -> LazyFrame vide mais valide (premier run)
/// - sans schéma     -> erreur
pub fn read_table_parquet<P: AsRef<Path>>(
    path: P,
    schema: Option<Schema>,
) -> PolarsResult<LazyFrame> {
    let path = path.as_ref();

    if !path.exists() {
        return match schema {
            Some(schema) => Ok(DataFrame::empty_with_schema(&schema).lazy()),
            None => Err(polars_err!(
                ComputeError: "Path does not exist: {}", path.display()
            )),
        };
    }

    LazyFrame::scan_parquet(to_pl_path(path)?, ScanArgsParquet::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use tempfile::tempdir;

    // --- Helpers ---

    fn sample_schema() -> Schema {
        Schema::from_iter(vec![
            Field::new("_pk_hash".into(), DataType::UInt64),
            Field::new("value".into(), DataType::String),
        ])
    }

    fn write_test_parquet(path: &Path, hashes: &[u64]) {
        let values = vec!["test"; hashes.len()];
        let mut df = df!(
            "_pk_hash" => hashes,
            "value" => values
        )
        .unwrap();

        let mut file = File::create(path).unwrap();
        ParquetWriter::new(&mut file).finish(&mut df).unwrap();
    }

    // --- 1. Lecture de fichiers existants ---

    #[test]
    fn reads_existing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.parquet");
        write_test_parquet(&path, &[1, 2, 3]);

        let df = read_table_parquet(&path, None).unwrap().collect().unwrap();

        assert_eq!(df.height(), 3);
        assert_eq!(df.width(), 2);
    }

    #[test]
    fn reads_directory_of_parquet_files() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("partitioned_data");
        std::fs::create_dir_all(&data_dir).unwrap();

        write_test_parquet(&data_dir.join("part-0.parquet"), &[1, 2]);
        write_test_parquet(&data_dir.join("part-1.parquet"), &[3, 4, 5]);

        let df = read_table_parquet(&data_dir, None)
            .unwrap()
            .collect()
            .unwrap();

        assert_eq!(df.height(), 5);
    }

    #[test]
    fn accepts_str_and_pathbuf() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.parquet");
        write_test_parquet(&path, &[1]);

        assert!(read_table_parquet(path.clone(), None).is_ok());
        assert!(read_table_parquet(path.to_str().unwrap(), None).is_ok());
    }


    #[test]
    fn missing_file_with_schema_returns_empty_frame() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("does_not_exist.parquet");
        let schema = sample_schema();

        let df = read_table_parquet(&path, Some(schema.clone()))
            .unwrap()
            .collect()
            .unwrap();

        assert_eq!(df.height(), 0);
        assert_eq!(df.width(), 2);
        assert_eq!(df.schema().as_ref(), &schema);
    }

    // --- 3. Lazy evaluation ---

    #[test]
    fn filter_and_select_work_lazily() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.parquet");
        write_test_parquet(&path, &[1, 2, 3, 4, 5]);

        let df = read_table_parquet(&path, None)
            .unwrap()
            .filter(col("_pk_hash").gt(lit(3u64)))
            .select([col("_pk_hash")])
            .collect()
            .unwrap();

        assert_eq!(df.height(), 2); // hashes 4 et 5
        assert_eq!(df.width(), 1);
    }

    // --- 4. Cycle complet : premier run puis second run ---

    #[test]
    fn first_run_then_second_run() {
        let dir = tempdir().unwrap();
        let dest = dir.path().join("curated.parquet");

        // Premier run : la table n'existe pas, on obtient une table vide
        let existing = read_table_parquet(&dest, Some(sample_schema()))
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(existing.height(), 0);

        // On écrit les premières données
        write_test_parquet(&dest, &[1, 2]);

        // Second run : la table existe, pas besoin de schéma
        let after = read_table_parquet(&dest, None).unwrap().collect().unwrap();
        assert_eq!(after.height(), 2);
    }
}