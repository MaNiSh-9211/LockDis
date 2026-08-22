//! Postgres fencing adapter: generates the conditional-update SQL that
//! makes stale holders' writes rejectable (ADR 0005 made practical).
//!
//! Zero I/O by design — wire the generated SQL into any Postgres driver.
//! Pattern for `tokio-postgres`:
//!
//! ```text
//! let sql = fenced_update("orders", &["status = $2"], "id");
//! client.execute(&sql, &[&order_id, &new_status, &fence.value()])?;
//! // 0 rows updated ⇒ your lease was stale; abort the critical section.
//! ```

/// Ensures the fence column exists on a table (run once per deployment).
pub fn ensure_fence_column(table: &str) -> String {
    assert_identifier(table);
    format!("ALTER TABLE {table} ADD COLUMN IF NOT EXISTS last_fence BIGINT NOT NULL DEFAULT 0")
}

/// Builds a fenced UPDATE: assignments apply only when the caller's fence
/// is strictly newer than the last accepted one. `assignments` are bare
/// column names; values bind positionally starting at `$2` in the same
/// order, with the fencing token as the final parameter.
///
/// Returns `(sql, fence_param_index)`.
pub fn fenced_update(table: &str, id_col: &str, assignments: &[&str]) -> (String, usize) {
    assert_identifier(table);
    assert_identifier(id_col);
    for a in assignments {
        assert!(!a.is_empty(), "empty assignment");
    }

    // Assignment values start at $2 (id is $1); the fence lands last.
    let set_parts: Vec<String> = assignments
        .iter()
        .enumerate()
        .map(|(i, a)| format!("{a} = ${}", i + 2))
        .collect();
    let fence_idx = assignments.len() + 2;
    let sql = format!(
        "UPDATE {table} SET {}, last_fence = ${fence_idx} \
         WHERE {id_col} = $1 AND ${fence_idx} > last_fence",
        set_parts.join(", ")
    );
    (sql, fence_idx)
}

/// Read-side check: is this fence newer than what the row has accepted?
/// Returns SQL taking `$1` = id, `$2` = fence; one row ⇒ proceed.
pub fn fenced_select(table: &str, id_col: &str, value_cols: &[&str]) -> String {
    assert_identifier(table);
    assert_identifier(id_col);
    let cols = if value_cols.is_empty() {
        "last_fence".to_owned()
    } else {
        format!("{}, last_fence", value_cols.join(", "))
    };
    format!("SELECT {cols} FROM {table} WHERE {id_col} = $1 AND $2 > last_fence")
}

fn assert_identifier(id: &str) {
    assert!(
        !id.is_empty()
            && id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.'),
        "identifier must be [A-Za-z0-9_.]: {id}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_column_sql() {
        assert_eq!(
            ensure_fence_column("orders"),
            "ALTER TABLE orders ADD COLUMN IF NOT EXISTS last_fence BIGINT NOT NULL DEFAULT 0"
        );
    }

    #[test]
    fn fenced_update_shapes_params() {
        let (sql, fence_idx) = fenced_update("orders", "id", &["status", "amount"]);
        assert_eq!(
            sql,
            "UPDATE orders SET status = $2, amount = $3, last_fence = $4 WHERE id = $1 AND $4 > last_fence"
        );
        assert_eq!(fence_idx, 4);
    }

    #[test]
    fn fenced_select_shapes() {
        let sql = fenced_select("orders", "id", &["status"]);
        assert_eq!(
            sql,
            "SELECT status, last_fence FROM orders WHERE id = $1 AND $2 > last_fence"
        );
    }

    #[test]
    #[should_panic(expected = "identifier")]
    fn rejects_injection_in_table() {
        fenced_update("orders; DROP TABLE users", "id", &["x = "]);
    }
}
