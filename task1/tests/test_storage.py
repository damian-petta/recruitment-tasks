import pytest
import os
import sys
sys.path.append("..")
from app.storage import retrieve_environment_variables,get_files_list

@pytest.fixture
def mock_environment_variables():
    os.environ["ENVIRONMENT"] = "TEST"
    os.environ["INPUT_RELATIVE_PATH"] = "../tests/data/input/"
    os.environ["OUTPUT_RELATIVE_PATH"] = "../tests/data/output/"
    yield  

    del os.environ["ENVIRONMENT"]
    del os.environ["INPUT_RELATIVE_PATH"]
    del os.environ["OUTPUT_RELATIVE_PATH"]



def test_retrieve_environment_variables(mock_environment_variables):
    info = retrieve_environment_variables()
    assert info.env == "test"
    current_directory = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.normpath(
        os.path.join(current_directory, "../tests/data/input/")
    )
    output_path = os.path.normpath(
        os.path.join(current_directory, "../tests/data/output/")
    )
    assert info.input_path == input_path
    assert info.output_path == output_path


def test_get_files_list(mock_environment_variables):
    info = retrieve_environment_variables()
    files_list = get_files_list(info.input_path, "tsv")
    assert len(files_list) == 1
