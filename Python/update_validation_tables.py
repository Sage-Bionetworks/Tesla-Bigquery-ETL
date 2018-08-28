
import synapseclient
import validation_updating as vu
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--input_synapse_id",
                    type=str)
parser.add_argument("--tables",
                    nargs="+",
                    type=str,
                    default="all",
                    required=False)
parser.add_argument("--dataset",
                    type=str,
                    default="Version_2",
                    required=False)

args = parser.parse_args()


ALL_TABLES = [
    "MHC_Binding_Assay",
    "TCR_Binding_by_Flow_I",
    "TCR_Binding_by_Flow_II",
    "TCR_Binding_by_Microfluidics",
    "MS_Peptide_IDs",
    "T_Cell_Reactivity_Screen"]


syn = synapseclient.Synapse()
syn.login()
entity = syn.get(args.input_synapse_id)
path = entity.path

if args.tables == "all":
    tables = ALL_TABLES
else:
    tables = args.tables

for table in tables:
    update_function = vu.TABLE_TO_FUNCTION[table]
    update_function(path, dataset=args.dataset)
