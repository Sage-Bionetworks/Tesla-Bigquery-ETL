bq load --null_marker=NA --skip_leading_rows=1 --source_format=CSV Version_3.Protein_Positions round1_protein_positions.csv
bq load --null_marker=NA --skip_leading_rows=1 --source_format=CSV Version_3.Prediction_Variants round1_prediction_variants.csv
bq load --null_marker=NA --skip_leading_rows=1 --source_format=CSV Version_3.Bad_Predictions round1_bad_predictions.csv
bq load --null_marker=NA --skip_leading_rows=1 --source_format=CSV Version_3.Predictions round1_predictions.csv


