gsutil mv roundx*.csv  gs://version_3
bq load --null_marker=NA --skip_leading_rows=1 --source_format=CSV Version_3.Protein_Positions gs://version_3/round3_protein_positions.csv
bq load --null_marker=NA --skip_leading_rows=1 --source_format=CSV Version_3.Predictions gs://version_3/round3_predictions.csv
bq load --null_marker=NA --skip_leading_rows=1 --source_format=CSV Version_3.Bad_Predictions gs://version_3/round3_bad_predictions.csv
bq load --null_marker=NA --skip_leading_rows=1 --source_format=CSV Version_3.Prediction_Variants gs://version_3/round3_prediction_variants.csv
