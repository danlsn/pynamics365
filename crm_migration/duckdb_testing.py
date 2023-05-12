import duckdb

crm_duckdb = "../data/duckdb.db"
sl_duckdb = "../data/salesloft_duck.db"

with duckdb.connect(crm_duckdb) as con:
    crm = con.execute("SHOW tables").fetchdf()
    for table in crm['name']:
        if "_L4" in table or "_L3" in table:
            con.execute(f"drop table {table}")
    # lead_l3 = con.execute("select * from lead_L3_template").fetchdf()
    # lead_l3.to_csv("../data/cleaned/lead_L3_Template.csv", index=False)
    print(crm.head())
