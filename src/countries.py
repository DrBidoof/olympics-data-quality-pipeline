import great_expectations as gx
import pandas as pd

CSV_PATH = "C:\\Users\\dartb\\OneDrive\\Documents\\health infomatics\\projects\\python\\1.olympic pipe line\\olympics-data-quality-pipeline\\data\\sample\\countries_sample.csv"
SUITE_NAME = "countries_suite"

def main():
    context = gx.get_context(mode="file");

    df = pd.read_csv(CSV_PATH)

    batch = context.data_sources.add_pandas(
        name="pandas_source_2"
        ).read_dataframe(df)

    validator = context.get_validator(
        batch=batch,
        expectation_suite_name=SUITE_NAME
        )

    #batch = context.sources.pandas_default.read_csv(CSV_PATH)


    validator.expect_table_columns_to_match_ordered_list(
        ["Country", "Code", "Population", "GDP per Capita"]
        )
    

    validator.expect_column_values_to_not_be_null("Country")

    validator.expect_column_values_to_be_unique("Country")

    validator.expect_column_values_to_not_be_null("Code")
    validator.expect_column_values_to_be_unique("Code")

    validator.expect_column_values_to_match_regex("Code", r"^[A-Z]{3}$")

    # Population: optional; if present must be reasonable
    validator.expect_column_values_to_be_between(
        "Population", min_value=1, max_value=2_000_000_000, mostly=0.99
    )

    # GDP per Capita: optional; if present must be > 0 and sane
    validator.expect_column_values_to_be_between(
        "GDP per Capita", min_value=1, max_value=300_000, mostly=0.99
    )

    # Save suite + run validation
    validator.save_expectation_suite(discard_failed_expectations=False)
    results = validator.validate()

    # Build Data Docs (HTML report)
    context.build_data_docs()

    print(f"Validation successful: {results.success} ")
    print("open data docs here:")
    print(f"gx/uncommitted/data_docs/local_site/index.html")

if __name__ == "__main__":
    main()




