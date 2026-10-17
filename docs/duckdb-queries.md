# Querying event bundles with DuckDB

Each confirmed event leaves a bundle in `bundles/<event_id>/`: Parquet tables
sliced from AEMO's files for the event window (start minus 2 h to end plus
1 h), and a `manifest.json` that lists every source file with its NEMWeb URL
and SHA-256. DuckDB reads the files directly and takes no locks, so analysis
can run while the monitor writes new bundles. Do not point DuckDB at the
monitor's SQLite database; that is operational state, not the record.

| File | AEMO table | When |
| --- | --- | --- |
| `dispatch_price.parquet` | DISPATCH.PRICE | when the window has passed |
| `dispatch_regionsum.parquet` | DISPATCH.REGIONSUM | " |
| `dispatch_constraint.parquet` | DISPATCH.CONSTRAINT | " |
| `dispatch_interconnectorres.parquet` | DISPATCH.INTERCONNECTORRES | " |
| `unit_dispatch.parquet` | DISPATCH.UNIT_SOLUTION (Next_Day_Dispatch) | after next-day data (about 04:15 AEST) |
| `offer_trk.parquet` | DISPATCH.OFFERTRK: the offer version dispatch used | " |
| `bids_dayoffer.parquet` | BIDS.BIDDAYOFFER: price bands, every version for the trading day | " |
| `bids_offerperiod.parquet` | BIDS.BIDOFFERPERIOD_SPARSE: band availability, runs of periods overlapping the window | " |

Column names are AEMO's. Timestamps are naive market time (AEST, UTC+10, no
daylight saving) and mark the end of each 5-minute interval. Rows from both
dispatch runs are kept: filter `INTERVENTION = 0` for the pricing run.

The figures in the comments below come from the bundle for the NSW event of
12 October 2026 (`202610121420-NSW1-high`).

## The event in one query

```sql
-- Peak, intervals above $5,000/MWh and time-weighted average price.
SELECT max(RRP) AS peak,
       count(*) FILTER (WHERE RRP >= 5000) AS intervals_above_5000,
       avg(RRP) FILTER (WHERE SETTLEMENTDATE BETWEEN '2026-10-12 14:20' AND '2026-10-12 15:35') AS mean_in_event
FROM 'bundles/202610121420-NSW1-high/dispatch_price.parquet'
WHERE REGIONID = 'NSW1' AND INTERVENTION = 0;
-- peak 23107, intervals_above_5000 11
```

## Price, demand and interchange by interval

```sql
SELECT p.SETTLEMENTDATE, p.REGIONID, p.RRP, r.TOTALDEMAND, r.AVAILABLEGENERATION, r.NETINTERCHANGE
FROM 'bundles/202610121420-NSW1-high/dispatch_price.parquet' p
JOIN 'bundles/202610121420-NSW1-high/dispatch_regionsum.parquet' r
  USING (SETTLEMENTDATE, REGIONID, INTERVENTION)
WHERE p.INTERVENTION = 0 AND p.REGIONID = 'NSW1'
ORDER BY p.SETTLEMENTDATE;
```

## Binding constraints

```sql
-- Constraints with a non-zero marginal value during the event, most often binding first.
SELECT CONSTRAINTID,
       count(*) AS binding_intervals,
       max(abs(MARGINALVALUE)) AS max_abs_marginal
FROM 'bundles/202610121420-NSW1-high/dispatch_constraint.parquet'
WHERE INTERVENTION = 0 AND MARGINALVALUE <> 0
  AND SETTLEMENTDATE BETWEEN '2026-10-12 14:20' AND '2026-10-12 15:35'
GROUP BY ALL
ORDER BY binding_intervals DESC, max_abs_marginal DESC
LIMIT 15;
```

## Interconnectors at their limits

```sql
SELECT SETTLEMENTDATE, INTERCONNECTORID, MWFLOW, EXPORTLIMIT, IMPORTLIMIT, MARGINALVALUE
FROM 'bundles/202610121420-NSW1-high/dispatch_interconnectorres.parquet'
WHERE INTERVENTION = 0 AND (MWFLOW >= EXPORTLIMIT - 1 OR MWFLOW <= IMPORTLIMIT + 1)
ORDER BY SETTLEMENTDATE, INTERCONNECTORID;
```

## The offer each unit was dispatched on

`offer_trk` names the offer version (`BIDOFFERDATE`) that dispatch used for
each unit and interval; join it to the bid tables on that time.

```sql
SELECT t.SETTLEMENTDATE, t.DUID, d.PRICEBAND1, d.PRICEBAND10, d.REBIDEXPLANATION
FROM 'bundles/202610121420-NSW1-high/offer_trk.parquet' t
JOIN 'bundles/202610121420-NSW1-high/bids_dayoffer.parquet' d
  ON d.DUID = t.DUID AND d.BIDTYPE = t.BIDTYPE AND d.OFFERDATE = t.BIDOFFERDATE
WHERE t.BIDTYPE = 'ENERGY' AND t.SETTLEMENTDATE = '2026-10-12 14:20'
ORDER BY t.DUID
LIMIT 20;
```

Bidirectional units (batteries) bid `GEN` and `LOAD` separately: add
`DIRECTION` to the join when it matters.

## Band availability in an interval

Period 1 ends at 04:05 on the trading date, so the interval ending 14:20 is
period `(14:20 - 04:00) / 5 = 124`.

```sql
SELECT DUID, DIRECTION, OFFERDATETIME, MAXAVAIL, BANDAVAIL1, BANDAVAIL10
FROM 'bundles/202610121420-NSW1-high/bids_offerperiod.parquet'
WHERE BIDTYPE = 'ENERGY' AND 124 BETWEEN PERIODID AND PERIODIDTO
  AND DUID = 'BW01'
ORDER BY OFFERDATETIME DESC
LIMIT 5;
```

## Across every bundle

```sql
-- Every event's NSW1 peak, from all bundles at once.
SELECT regexp_extract(filename, 'bundles[/\\]([^/\\]+)[/\\]', 1) AS event_id,
       REGIONID, max(RRP) AS peak
FROM read_parquet('bundles/*/dispatch_price.parquet', filename = true)
WHERE INTERVENTION = 0
GROUP BY ALL
ORDER BY peak DESC;
```

## Checking provenance

`scripts/bundle_check.py` recomputes every hash in a manifest, compares
source hashes with original AEMO files you supply, and reproduces the peak
with DuckDB:

```sh
python3 scripts/bundle_check.py bundles/202610121420-NSW1-high \
  --sources path/to/aemo/files --expect-peak 23107 --expect-above 11
```
