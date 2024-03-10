import pytest
from unittest.mock import MagicMock
from your_module import main

@pytest.fixture
def mock_session():
    return MagicMock()

def test_main_successful_creation(mock_session):
    # Mocking session.table method
    mock_session.table.return_value.filter.return_value.create_or_replace_view.return_value = MagicMock()
    result = main(mock_session)
    assert result is not None
    assert result.startswith("W_STUDENTS")
    mock_session.table.assert_called_once_with("STUDENTS_STAGING.STUDENTS_MERGED_DB.FINAL_MERGED")
    mock_session.table.return_value.filter.assert_called_once()
    mock_session.table.return_value.filter.return_value.create_or_replace_view.assert_called_once_with("STUDENTS_SEMANTIC.VIEWS_STUDENTS.W_STUDENTS")
    
def test_main_creation_failure(mock_session):
    # Mocking session.table method to raise an exception
    mock_session.table.side_effect = Exception("Test Exception")
    with pytest.raises(Exception):
        main(mock_session)
