bq mk --table Version_3.Protein_Positions json/Protein_Positions.json
bq mk --table Version_3.Prediction_Variants json/Prediction_Variants.json
bq mk --table Version_3.Predictions json/Predictions.json
bq mk --table Version_3.Bad_Predictions json/Predictions.json
bq mk --table Version_3.Steps json/Steps.json
bq mk --table Version_3.Parameters json/Parameters.json
bq mk --table Version_3.Variants json/Variants.json
bq mk --table Version_3.Submissions json/Submissions.json

bq mk --table Version_3.Teams json/Teams.json
bq mk --table Version_3.Survey_Answers json/Survey_Answers.json
bq mk --table Version_3.Survey_Questions json/Survey_Questions.json

bq mk --table Version_3.Patients json/Patients.json
bq mk --table Version_3.Patient_Alleles json/Patient_Alleles.json
bq mk --table Version_3.Samples json/Samples.json
bq mk --table Version_3.Validated_Bindings json/Validated_Bindings.json
bq mk --table Version_3.Validated_Epitopes json/Validated_Epitopes.json

bq mk --table Version_3.Variant_Overlap json/Variant_Overlap.json
bq mk --table Version_3.CNVKit_Segment_Genes json/CNVKit_Segment_Genes.json
bq mk --table Version_3.CNVKit_Segments json/CNVKit_Segments.json
