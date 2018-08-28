
"""
Functions for updating validation tables.
"""

import pandas as pd
import validation_proccessing as vp
from pandas.io import gbq
from functools import partial


def insert_validation_table(input_csv_path, table, output_columns,
                            input_columns=None, dataset="Version_2",
                            project="neoepitopes"):
    """Read input file, process, and insert into bigquery table."""
    if input_columns is None:
        input_columns = output_columns
    df = pd.read_csv(input_csv_path)
    df = vp.process_validation_table(df, input_columns, output_columns)
    table_name = ".".join([dataset, table])
    gbq.to_gbq(df, table_name, project, if_exists='append')


insert_mhc_binding_assay_table = partial(
    insert_validation_table,
    output_columns=[
        "PATIENT", "ALT_EPI_SEQ", "HLA_ALLELE", "MHC_BINDING_ASSAY"],
    table="MHC_Binding_Assay"
)

insert_tcr_binding_by_flow_i_table = partial(
    insert_validation_table,
    output_columns=[
        "PATIENT", "ALT_EPI_SEQ", "HLA_ALLELE", "TCR_BINDING_BY_FLOW_I"],
    table="TCR_Binding_by_Flow_I"
)

insert_tcr_binding_by_flow_ii_table = partial(
    insert_validation_table,
    output_columns=[
        "PATIENT", "ALT_EPI_SEQ", "HLA_ALLELE", "TCR_BINDING_BY_FLOW_II"],
    table="TCR_Binding_by_Flow_II"
)

insert_tcr_binding_by_microfluidics_table = partial(
    insert_validation_table,
    output_columns=[
        "PATIENT", "ALT_EPI_SEQ", "HLA_ALLELE",
        "TCR_BINDING_BY_MICROFLUIDICS"],
    table="TCR_Binding_by_Microfluidics"
)

insert_ms_peptide_ids_table = partial(
    insert_validation_table,
    output_columns=[
        "PATIENT", "ALT_EPI_SEQ", "MS_PEPTIDE_ID"],
    table="MS_Peptide_IDs"
)

insert_t_cell_reactivity_screen_table = partial(
    insert_validation_table,
    output_columns=[
        "PATIENT", "ALT_EPI_SEQ", "T_CELL_REACTIVITY_SCREEN"],
    table="T_Cell_Reactivity_Screen"
)

TABLE_TO_FUNCTION = {
    "MHC_Binding_Assay": insert_mhc_binding_assay_table,
    "TCR_Binding_by_Flow_I": insert_tcr_binding_by_flow_i_table,
    "TCR_Binding_by_Flow_II": insert_tcr_binding_by_flow_ii_table,
    "TCR_Binding_by_Microfluidics": insert_tcr_binding_by_microfluidics_table,
    "MS_Peptide_IDs": insert_ms_peptide_ids_table,
    "T_Cell_Reactivity_Screen": insert_t_cell_reactivity_screen_table,
}
