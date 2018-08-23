
"""
Functions for processing validation tables
"""

import string
import pandas as pd
from functools import partial


def process_validation_table(df, input_columns, output_columns):
    """Process validation data from input df."""
    df = filter_df_columns(df, input_columns, output_columns)
    df = format_column_values(df)
    df = df.dropna()
    df = df.drop_duplicates(keep="first")
    return(df)


def filter_df_columns(df, input_columns, output_columns):
    """Filter and rename columns in df."""
    if(len(input_columns) != len(output_columns)):
        raise Exception("Input and output coulmns not equal")
    if(len([col for col in input_columns if col not in df.columns]) != 0):
        raise Exception("Input columns missing from input df")
    df = df.loc[:, input_columns]
    df.columns = output_columns
    return(df)


def format_column_values(df):
    """Format all column values."""
    for column in df.columns:
        if column not in COLUMN_TO_FUNCTION.keys():
            raise Exception("Column has no function for processing.")
        func = COLUMN_TO_FUNCTION[column]
        df = func(df)
    return(df)


# functions for processing individual values of columns


def format_alt_epi_seq_value(value):
    """Format value in ALT EPI SEQ column."""
    s = ''.join([letter for letter in value if letter.isalpha()])
    return(s.upper())


def format_patient_value(value):
    """Format value in PATIENT column."""
    s = ''.join([letter for letter in value if letter.isdigit()])
    return("Patient_" + s)


def format_hla_allele_value(value):
    """Format value in HLA ALLELE column."""
    translator = str.maketrans('', '', string.punctuation)
    s = value.upper()
    s = s.split(";")[0]
    s = s.replace('HLA-', '')
    return(s.translate(translator))


def enforce_enumeration_value(value, allowed_values):
    """Change the value to None if not in list of allowed values."""
    if value in allowed_values:
        return(value)
    else:
        return(None)

# functions for processing columns


def process_column_by_function(df, column, func):
    """Apply function to each value in df column."""
    df[column] = df[column].apply(lambda x: func(x))
    return(df)


process_patient_column = partial(
    process_column_by_function,
    column="PATIENT",
    func=format_patient_value)

process_alt_epi_seq_column = partial(
    process_column_by_function,
    column="ALT_EPI_SEQ",
    func=format_alt_epi_seq_value)

process_hla_allele_column = partial(
    process_column_by_function,
    column="HLA_ALLELE",
    func=format_hla_allele_value)


def coerce_column_to_int(df, column):
    """Coerces column in df to integer."""
    df[column] = pd.to_numeric(df[column], errors='coerce').round()
    df = df.dropna()
    df[column] = df[column].astype('int')
    return(df)


process_ms_peptide_id_column = partial(
    coerce_column_to_int, column="MS_PEPTIDE_ID")

process_binding_assay_result_column = partial(
    coerce_column_to_int, column="BINDING_ASSAY_RESULT")


def enforce_enumeration_on_column(df, column, values):
    """Enforce column is an enumeration of items in values arg."""
    df[column] = df[column].apply(
        lambda x: enforce_enumeration_value(x, values))
    return(df)


process_flow1_assay_result_column = partial(
    enforce_enumeration_on_column,
    column="FLOW1_ASSAY_RESULT",
    values=["+", "-"])

process_flow2_assay_result_column = partial(
    enforce_enumeration_on_column,
    column="FLOW2_ASSAY_RESULT",
    values=["+", "-"])

process_microfluids_assay_result_column = partial(
    enforce_enumeration_on_column,
    column="MICROFLUIDCS_ASSAY_RESULT",
    values=["+", "-"])

process_reactivity_assay_result_column = partial(
    enforce_enumeration_on_column,
    column="REACTIVITY_ASSAY_RESULT",
    values=["+", "-"])

COLUMN_TO_FUNCTION = {
    "PATIENT": process_patient_column,
    "ALT_EPI_SEQ": process_alt_epi_seq_column,
    "HLA_ALLELE": process_hla_allele_column,
    "MS_PEPTIDE_ID": process_ms_peptide_id_column,
    "BINDING_ASSAY_RESULT": process_binding_assay_result_column,
    "FLOW1_ASSAY_RESULT": process_flow1_assay_result_column,
    "FLOW2_ASSAY_RESULT": process_flow2_assay_result_column,
    "MICROFLUIDCS_ASSAY_RESULT": process_microfluids_assay_result_column,
    "REACTIVITY_ASSAY_RESULT": process_reactivity_assay_result_column,
}
