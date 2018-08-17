
'''
docstring
'''

import unittest
import pandas as pd
import validation_proccessing as vp
from pandas.util.testing import assert_frame_equal


class TestProcessingDF(unittest.TestCase):
    """Docstring."""

    def setUp(self):
        """Docstring."""
        self.test_df1 = pd.DataFrame({
            "pat": ["P4", "pat8"],
            "epi": ["asssd", "ghGG"],
            "hla": ["HLA-A0201", "HLA-B0201"],
            "assay1": ["+", "-"],
            "assay2": [5, 10],
            "assay3": ["+", "++"]})

    def test_df1(self):
        """Docstring."""
        assert_frame_equal(
            vp.process_validation_table(
                self.test_df1,
                ["pat", "epi", "hla", "assay2"],
                ["PATIENT", "ALT_EPI_SEQ", "HLA_ALLELE",
                 "BINDING_ASSAY_RESULT"]),
            pd.DataFrame({
                "PATIENT": ["Patient_4", "Patient_8"],
                "ALT_EPI_SEQ": ["ASSSD", "GHGG"],
                "HLA_ALLELE": ["A0201", "B0201"],
                "BINDING_ASSAY_RESULT": [5, 10]}),
            check_like=True)


class TestProcessingColumns(unittest.TestCase):
    """Docstring."""

    def setUp(self):
        """Docstring."""
        self.test_df1 = pd.DataFrame(
            {"col1": ["val1", "val2"],
             "col2": ["val1", "val2"],
             "col3": ["val1", "val2"]})

    def test_format_column_values(self):
        """Docstring."""
        assert_frame_equal(
            vp.format_column_values(
                pd.DataFrame({"MS_PEPTIDE_ID": ["1", "2"]})),
            pd.DataFrame({"MS_PEPTIDE_ID": [1, 2]}))
        assert_frame_equal(
            vp.format_column_values(
                pd.DataFrame({"FLOW1_ASSAY_RESULT": ["+", "na"]})),
            pd.DataFrame({"FLOW1_ASSAY_RESULT": ["+", None]}))
        with self.assertRaises(Exception):
            vp.format_column_values(
                pd.DataFrame({"FLOW3_ASSAY_RESULT": ["+", "na"]}))

    def test_coerce_column_to_int(self):
        """Docstring."""
        assert_frame_equal(vp.coerce_column_to_int(
            pd.DataFrame({"col": ["1", "2"]}), "col"),
            pd.DataFrame({"col": [1, 2]}))
        assert_frame_equal(vp.coerce_column_to_int(
            pd.DataFrame({"col": [1, 2]}), "col"),
            pd.DataFrame({"col": [1, 2]}))
        assert_frame_equal(vp.coerce_column_to_int(
            pd.DataFrame({"col": [1.0, 2.0]}), "col"),
            pd.DataFrame({"col": [1, 2]}))
        assert_frame_equal(vp.coerce_column_to_int(
            pd.DataFrame({"col": [1.1, 2.1]}), "col"),
            pd.DataFrame({"col": [1, 2]}))
        assert_frame_equal(vp.coerce_column_to_int(
            pd.DataFrame({"col": [1.9, 2.9]}), "col"),
            pd.DataFrame({"col": [2, 3]}))
        with self.assertRaises(Exception):
            vp.coerce_column_to_int(
                pd.DataFrame({"col": ["val1", "val2"]}), "col")

    def test_enforce_enumeration_on_column(self):
        """Docstring."""
        assert_frame_equal(vp.enforce_enumeration_on_column(
            pd.DataFrame({"col": ["val1", "val2"]}), "col", ["val1", "val2"]),
            pd.DataFrame({"col": ["val1", "val2"]}))
        assert_frame_equal(vp.enforce_enumeration_on_column(
            pd.DataFrame({"col": ["val1", "val2"]}), "col", ["val1"]),
            pd.DataFrame({"col": ["val1", None]}))

    def test_filter_df_columns(self):
        """Docstring."""
        assert_frame_equal(vp.filter_df_columns(
            self.test_df1, ["col1", "col2"], ["COL1", "COL2"]),
            pd.DataFrame({
                "COL1": ["val1", "val2"],
                "COL2": ["val1", "val2"]}))
        assert_frame_equal(vp.filter_df_columns(
            self.test_df1, ["col2", "col1"], ["COL2", "COL1"]),
            pd.DataFrame({
                "COL2": ["val1", "val2"],
                "COL1": ["val1", "val2"]},
                columns=['COL2', 'COL1']))
        with self.assertRaises(Exception):
            vp.filter_df_columns(self.test_df1,
                                 ["col1", "col2"],
                                 ["COL1"])
        with self.assertRaises(Exception):
            vp.filter_df_columns(self.test_df1,
                                 ["col1"],
                                 ["COL1", "COL2"])
        with self.assertRaises(Exception):
            vp.filter_df_columns(self.test_df1,
                                 ["col1", "col4"],
                                 ["COL1", "COL4"])


class TestProcessingValues(unittest.TestCase):
    """Docstring."""

    def test_format_alt_epi_seq_value(self):
        """Docstring."""
        assert vp.format_alt_epi_seq_value("AAA") == "AAA"
        assert vp.format_alt_epi_seq_value("aaa") == "AAA"
        assert vp.format_alt_epi_seq_value("abc1") == "ABC"

    def test_format_patient_value(self):
        """Docstring."""
        assert vp.format_patient_value("TESLA_P1") == "Patient_1"
        assert vp.format_patient_value("tesla_p2") == "Patient_2"
        assert vp.format_patient_value("TESLA_P10") == "Patient_10"
        assert vp.format_patient_value("P20") == "Patient_20"
        assert vp.format_patient_value("p_30") == "Patient_30"
        assert vp.format_patient_value("40p") == "Patient_40"

    def test_format_hla_allele_value(self):
        """Docstring."""
        assert vp.format_hla_allele_value("A02:01") == "A0201"
        assert vp.format_hla_allele_value("A02:01; C03:03") == "A0201"
        assert vp.format_hla_allele_value("A02:01;C03:03") == "A0201"
        assert vp.format_hla_allele_value("A*02:01") == "A0201"
        assert vp.format_hla_allele_value("a02:01") == "A0201"
        assert vp.format_hla_allele_value("A0201") == "A0201"
        assert vp.format_hla_allele_value("HLA-A0201") == "A0201"
        assert vp.format_hla_allele_value("HLA-A02:01; HLA-C03:03") == "A0201"

    def test_enforce_enumeration(self):
        """Docstring."""
        assert vp.enforce_enumeration_value("+", ["+", "-"]) == "+"
        assert vp.enforce_enumeration_value("-", ["+", "-"]) == "-"
        assert vp.enforce_enumeration_value("TBD", ["+", "-"]) is None
        assert vp.enforce_enumeration_value(5, ["+", "-"]) is None
        assert vp.enforce_enumeration_value(None, ["+", "-"]) is None


if __name__ == '__main__':
    unittest.main()
