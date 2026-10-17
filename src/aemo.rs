//! Parser for AEMO's multi-table CSV report format.
//!
//! Rows start with C (comment), I (header) or D (data). I and D rows carry the
//! report name, table name and version in columns 1 to 3; fields follow from
//! column 4. Columns are looked up by name so version changes that add fields
//! do not break parsing.

use std::collections::HashMap;

use anyhow::{Context, Result};

use crate::model::{ConstraintRow, DispatchSnapshot, InterconnectorRow, RegionInterval};
use crate::time::parse_market;

#[derive(Debug, Default, Clone)]
pub struct Table {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl Table {
    pub fn idx(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c == name)
    }

    pub fn col<'a>(&self, row: &'a [String], name: &str) -> Option<&'a str> {
        self.idx(name).and_then(|i| row.get(i)).map(|s| s.as_str())
    }

    pub fn num(&self, row: &[String], name: &str) -> Option<f64> {
        self.col(row, name)
            .and_then(|s| s.trim().parse::<f64>().ok())
    }
}

/// Parse one report into tables keyed "REPORT_TABLE", e.g. "DISPATCH_PRICE".
pub fn parse_report(bytes: &[u8]) -> Result<HashMap<String, Table>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(bytes);
    let mut tables: HashMap<String, Table> = HashMap::new();
    for rec in rdr.records() {
        let rec = rec.context("malformed CSV row")?;
        if rec.len() < 4 {
            continue;
        }
        let key = format!(
            "{}_{}",
            rec[1].trim().to_uppercase(),
            rec[2].trim().to_uppercase()
        );
        match rec[0].trim() {
            "I" => {
                let t = tables.entry(key).or_default();
                t.columns = rec
                    .iter()
                    .skip(4)
                    .map(|c| c.trim().to_uppercase())
                    .collect();
            }
            "D" => {
                if let Some(t) = tables.get_mut(&key) {
                    t.rows
                        .push(rec.iter().skip(4).map(|c| c.to_string()).collect());
                }
            }
            _ => {}
        }
    }
    Ok(tables)
}

fn is_pricing_run(t: &Table, row: &[String]) -> bool {
    // INTERVENTION = 0 is the pricing run, which sets the published price.
    t.col(row, "INTERVENTION")
        .map(|v| v.trim() == "0" || v.trim().is_empty())
        .unwrap_or(true)
}

/// Extract the fields the detector needs from a DispatchIS report.
pub fn dispatch_snapshot(tables: &HashMap<String, Table>) -> Result<DispatchSnapshot> {
    let price = tables
        .get("DISPATCH_PRICE")
        .context("report has no DISPATCH,PRICE table")?;
    let mut snap = DispatchSnapshot::default();
    let mut settlement = None;

    for row in &price.rows {
        if !is_pricing_run(price, row) {
            continue;
        }
        let (Some(region), Some(rrp), Some(sd)) = (
            price.col(row, "REGIONID"),
            price.num(row, "RRP"),
            price.col(row, "SETTLEMENTDATE").and_then(parse_market),
        ) else {
            continue;
        };
        settlement = Some(settlement.map_or(sd, |s: chrono::NaiveDateTime| s.max(sd)));
        snap.regions.insert(
            region.trim().to_string(),
            RegionInterval {
                rrp,
                ..Default::default()
            },
        );
    }
    snap.settlement = settlement.context("no priced regions in report")?;

    if let Some(rs) = tables.get("DISPATCH_REGIONSUM") {
        for row in &rs.rows {
            if !is_pricing_run(rs, row) {
                continue;
            }
            if let Some(r) = rs
                .col(row, "REGIONID")
                .and_then(|id| snap.regions.get_mut(id.trim()))
            {
                r.demand = rs.num(row, "TOTALDEMAND");
                r.available_gen = rs.num(row, "AVAILABLEGENERATION");
                r.net_interchange = rs.num(row, "NETINTERCHANGE");
            }
        }
    }

    if let Some(c) = tables.get("DISPATCH_CONSTRAINT") {
        for row in &c.rows {
            if !is_pricing_run(c, row) {
                continue;
            }
            let Some(id) = c.col(row, "CONSTRAINTID") else {
                continue;
            };
            snap.constraints.push(ConstraintRow {
                id: id.trim().to_string(),
                rhs: c.num(row, "RHS"),
                lhs: c.num(row, "LHS"),
                marginal: c.num(row, "MARGINALVALUE").unwrap_or(0.0),
                violation: c.num(row, "VIOLATIONDEGREE").unwrap_or(0.0),
            });
        }
    }

    if let Some(ic) = tables.get("DISPATCH_INTERCONNECTORRES") {
        for row in &ic.rows {
            if !is_pricing_run(ic, row) {
                continue;
            }
            let Some(id) = ic.col(row, "INTERCONNECTORID") else {
                continue;
            };
            snap.interconnectors.push(InterconnectorRow {
                id: id.trim().to_string(),
                flow: ic.num(row, "MWFLOW").unwrap_or(0.0),
                export_limit: ic.num(row, "EXPORTLIMIT"),
                import_limit: ic.num(row, "IMPORTLIMIT"),
                marginal: ic.num(row, "MARGINALVALUE").unwrap_or(0.0),
            });
        }
    }
    Ok(snap)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"C,NEMP.WORLD,DISPATCHIS,AEMO,PUBLIC,2026/10/12,14:30:13,0000000542000000,DISPATCHIS,0000000542000000
I,DISPATCH,PRICE,5,SETTLEMENTDATE,RUNNO,REGIONID,DISPATCHINTERVAL,INTERVENTION,RRP,EEP,ROP,APCFLAG
D,DISPATCH,PRICE,5,"2026/10/12 14:35:00",1,NSW1,20261012171,0,23107.0,0,23107.0,0
D,DISPATCH,PRICE,5,"2026/10/12 14:35:00",1,QLD1,20261012171,0,180.5,0,180.5,0
I,DISPATCH,REGIONSUM,9,SETTLEMENTDATE,RUNNO,REGIONID,DISPATCHINTERVAL,INTERVENTION,TOTALDEMAND,AVAILABLEGENERATION,NETINTERCHANGE
D,DISPATCH,REGIONSUM,9,"2026/10/12 14:35:00",1,NSW1,20261012171,0,12650.2,12980.0,-1400.0
I,DISPATCH,CONSTRAINT,5,SETTLEMENTDATE,RUNNO,CONSTRAINTID,DISPATCHINTERVAL,INTERVENTION,RHS,MARGINALVALUE,VIOLATIONDEGREE,LHS
D,DISPATCH,CONSTRAINT,5,"2026/10/12 14:35:00",1,N>>N-EXAMPLE_1,20261012171,0,850,-3200.5,0,850
D,DISPATCH,CONSTRAINT,5,"2026/10/12 14:35:00",1,N>>N-EXAMPLE_1,20261012171,1,850,-10,0,850
C,"END OF REPORT",9
"#;

    #[test]
    fn parses_dispatch_report() {
        let tables = parse_report(SAMPLE.as_bytes()).unwrap();
        let snap = dispatch_snapshot(&tables).unwrap();
        assert_eq!(snap.settlement.to_string(), "2026-10-12 14:35:00");
        assert_eq!(snap.regions["NSW1"].rrp, 23107.0);
        assert_eq!(snap.regions["NSW1"].demand, Some(12650.2));
        assert_eq!(snap.regions["QLD1"].demand, None);
        // The intervention run row (INTERVENTION=1) is skipped.
        assert_eq!(snap.constraints.len(), 1);
        assert_eq!(snap.constraints[0].marginal, -3200.5);
    }
}
