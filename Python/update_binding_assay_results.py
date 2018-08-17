import df_proccessing

# globals
BAR_OUTPUT_COLUMNS = [
    "PATIENT", "ALT_EPI_SEQ", "HLA_ALLELE", "BINDING_ASSAY_RESULT"]


def main():
    print("running2")
    pass


def process_binding_assay_results(df, input_columns, output_columns):
    df = filter_df_columns(df, input_columns, output_columns)
    df["PATIENT"] = [format_patient(s) for s in df["PATIENT"]]
    df["ALT_EPI_SEQ"] = [format_alt_epi_seq(s) for s in df["ALT_EPI_SEQ"]]
    df["HLA_ALLELE"] = [format_hla_allele(s) for s in df["HLA_ALLELE"]]
    df["BINDING_ASSAY_RESULT"] = pd.to_numeric(df["BINDING_ASSAY_RESULT"],
                                               errors='coerce')
    df = df.dropna()
    df = df.drop_duplicates(keep="first")
    return(df)


if __name__ == '__main__':
    print("running1")
    main()
