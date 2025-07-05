"""
Test JSON preprocessing functionality for PBS command output
"""

import pytest
from pbs_monitor.pbs_commands import PBSCommands


def test_json_preprocessing_malformed_numeric_values():
   """Test that malformed numeric values are properly fixed"""
   
   # Sample malformed JSON with unquoted large numeric values
   malformed_json = '''
   {
      "Jobs": {
         "12345.pbs-server": {
            "Variable_List": {
               "CI_COMMIT_BEFORE_SHA":0000000000000000000000000000000000000000,
               "ANOTHER_FIELD":"valid_value",
               "NUMERIC_FIELD":123456789012345678901234567890123456789012345,
               "NORMAL_FIELD":"normal_value"
            }
         }
      }
   }
   '''
   
   pbs_cmd = PBSCommands()
   
   # Test the preprocessing
   processed_json = pbs_cmd._preprocess_json(malformed_json)
   
   # The processed JSON should have quoted the large numeric values
   assert '"CI_COMMIT_BEFORE_SHA":"0000000000000000000000000000000000000000",' in processed_json
   assert '"NUMERIC_FIELD":"123456789012345678901234567890123456789012345",' in processed_json
   
   # Normal values should be unchanged
   assert '"ANOTHER_FIELD":"valid_value"' in processed_json
   assert '"NORMAL_FIELD":"normal_value"' in processed_json


def test_json_preprocessing_no_changes_needed():
   """Test that valid JSON is left unchanged"""
   
   valid_json = '''
   {
      "Jobs": {
         "12345.pbs-server": {
            "Variable_List": {
               "FIELD1":"value1",
               "FIELD2":"value2"
            }
         }
      }
   }
   '''
   
   pbs_cmd = PBSCommands()
   
   # Test the preprocessing
   processed_json = pbs_cmd._preprocess_json(valid_json)
   
   # Should be unchanged
   assert processed_json == valid_json


def test_json_parsing_with_preprocessing():
   """Test that complete JSON parsing works with preprocessing"""
   
   malformed_json = '''
   {
      "Jobs": {
         "12345.pbs-server": {
            "Variable_List": {
               "CI_COMMIT_BEFORE_SHA":0000000000000000000000000000000000000000,
               "NORMAL_FIELD":"normal_value"
            }
         }
      }
   }
   '''
   
   pbs_cmd = PBSCommands()
   
   # This should not raise an exception
   parsed_data = pbs_cmd._parse_json_output(malformed_json, "test")
   
   # Verify the structure is correct
   assert "Jobs" in parsed_data
   assert "12345.pbs-server" in parsed_data["Jobs"]
   assert "Variable_List" in parsed_data["Jobs"]["12345.pbs-server"]
   
   # Verify the values are correctly parsed
   variables = parsed_data["Jobs"]["12345.pbs-server"]["Variable_List"]
   assert variables["CI_COMMIT_BEFORE_SHA"] == "0000000000000000000000000000000000000000"
   assert variables["NORMAL_FIELD"] == "normal_value"


if __name__ == "__main__":
   # Run the tests
   test_json_preprocessing_malformed_numeric_values()
   test_json_preprocessing_no_changes_needed()
   test_json_parsing_with_preprocessing()
   print("All tests passed!") 