dbutils.widgets.text("audit_table",        "fraud_demo.silver.transactions_silver_audit")
dbutils.widgets.text("max_quarantine_pct", "5.0")

audit_table = dbutils.widgets.get("audit_table")
max_q_pct   = float(dbutils.widgets.get("max_quarantine_pct"))

latest = (
    spark.table(audit_table)
         .orderBy("run_ts", ascending=False)
         .limit(1)
         .collect()[0]
)

print("=" * 55)
print("  Silver audit check report")
print("=" * 55)
print(f"  Run ts:              {latest['run_ts']}")
print(f"  Total rows:          {latest['total_rows']:,}")
print(f"  Quarantine rows:     {latest['quarantine_rows']:,}")
print(f"  Quarantine pct:      {latest['quarantine_pct']:.2f}%")
print(f"  Sanctioned count:    {latest['sanctioned_count']:,}")
print(f"  High risk count:     {latest['high_risk_count']:,}")
print(f"  Cross border count:  {latest['cross_border_count']:,}")
print(f"  Total GBP volume:    £{latest['total_gbp_volume']:,.2f}")
print(f"  Audit status:        {latest['audit_status']}")
print("=" * 55)

assert latest["total_rows"] > 0, \
    "FATAL: silver table is empty"

assert latest["quarantine_pct"] <= max_q_pct, \
    (f"FATAL: quarantine rate {latest['quarantine_pct']:.2f}% "
     f"exceeds threshold {max_q_pct:.2f}%")

assert latest["sanctioned_count"] == 0, \
    (f"FATAL: {latest['sanctioned_count']} transactions with "
     f"sanctioned counterparties detected — immediate review required")

assert latest["audit_status"] == "PASS", \
    f"FATAL: silver audit status is {latest['audit_status']}"

print("\nAll checks passed. Silver layer is healthy.")