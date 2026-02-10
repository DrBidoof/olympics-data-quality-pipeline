import great_expectations as gx
import pandas as pd
from great_expectations.exceptions import DataContextError

CSV_PATH = r"C:\Users\dartb\OneDrive\Documents\health infomatics\projects\python\1.olympic pipe line\olympics-data-quality-pipeline\data\sample\countries_sample.csv"
SUITE_NAME = "countries_suite"
DS_NAME = "pandas_tmp"

def main():
    context = gx.get_context(mode="file")
    df = pd.read_csv(CSV_PATH, index_col=0)

    # get-or-create datasource
    try:
        ds = context.data_sources.get(DS_NAME)
    except Exception:
        ds = context.data_sources.add_pandas(name=DS_NAME)

    batch = ds.read_dataframe(df)

    # get-or-create suite (ensure it exists)
    try:
        context.suites.get(SUITE_NAME)
    except DataContextError:
        context.suites.add(gx.ExpectationSuite(name=SUITE_NAME))

    validator = context.get_validator(batch=batch, expectation_suite_name=SUITE_NAME)

    # expectations
    validator.expect_table_columns_to_match_ordered_list(
        ["Country", "Code", "Population", "GDP per Capita"]
    )
    validator.expect_column_values_to_not_be_null("Country")
    validator.expect_column_values_to_be_unique("Country")
    validator.expect_column_values_to_not_be_null("Code")
    validator.expect_column_values_to_be_unique("Code")
    validator.expect_column_values_to_match_regex("Code", r"^[A-Z]{3}$")
    validator.expect_column_values_to_be_between("Population", min_value=1, max_value=2_000_000_000, mostly=0.99)
    validator.expect_column_values_to_be_between("GDP per Capita", min_value=1, max_value=300_000, mostly=0.99)




    # overwrite/update suite safely (instead of validator.save_expectation_suite())
    suite = validator.get_expectation_suite()
    context.suites.add_or_update(suite)

    # validate
    results = validator.validate()
    validator.result_format = "SUMMARY"
    print("Validation successful:", results["success"])

    for r in results["results"]:
        if not r["success"]:
            print(r)

   

    # docs
    context.build_data_docs()
    context.open_data_docs()

if __name__ == "__main__":
    main()
