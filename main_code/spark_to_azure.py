from utils.cmutils import DatabaseOperations


db_ops = DatabaseOperations()


if __name__ == "__main__":

    # Read data from Oracle
    query = "SELECT * FROM DATAFILE30"
    df = db_ops.read_oracle_table(query)
    df.show()
    print(f"Total number of rows of dataset are :-", df.count())

    # Write full data to PostgreSQL
    db_ops.write_to_postgres(df, "datafile_combined")

    # Process and write individual tables to PostgreSQL
    db_ops.process_table(
        df,
        filter_col="TABLE_NUMBER",
        filter_val="BGII(LC)",
        select_cols=[("KEY1", "BGIITerritory"), ("KEY2", "BGIICoverage"), ("KEY3", "BGIISymbol"), ("FACTOR", "FactorBGII")],
        table_name="table1_BGIILC"
    )

    db_ops.process_table(
        df,
        filter_col="TABLE_NUMBER",
        filter_val="%B.(1)(LC)%",
        select_cols=[("FACTOR", "FactorB1LC")],
        table_name="table2_B1LC"
    )

    db_ops.process_table(
        df,
        filter_col="TABLE_NUMBER",
        filter_val="%C.(2)(LC)%",
        select_cols=[("KEY1", "Coverage"), ("FACTOR", "FactorC2LC")],
        table_name="table3_C2LC"
    )

    db_ops.process_table(
        df,
        filter_col="TABLE_NUMBER",
        filter_val="%85.(LC)%",
        select_cols=[("KEY1", "Class_code"), ("KEY2", "Coverage85LC"), ("KEY3", "Symbo85LC"), ("KEY4", "Constuction_Code"), ("FACTOR", "FactorBGII")],
        table_name="table4_85LC"
    )

    db_ops.process_table(
        df,
        filter_col="TABLE_NUMBER",
        filter_val="%85.TerrMult(LC)%",
        select_cols=[("KEY1", "Territory"), ("FACTOR", "Factor85TERR")],
        table_name="table5_85TerrMultLC"
    )

