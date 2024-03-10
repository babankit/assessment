import pytest
from unittest.mock import MagicMock
from your_module import get_student_df, get_missed_class_df, main

@pytest.fixture
def mock_session():
    return MagicMock()

def test_get_student_df(mock_session):
    expected_data = {"student_id": "123", "name": "John", "english_grade": 90, "history_grade": 85, "math_grade": 95, "science_grade": 88}
    mock_session.read.json.return_value.select.return_value.flatten.return_value.select_expr.return_value = MagicMock()
    result = get_student_df(mock_session)
    assert result is not None
    assert result == expected_data
    mock_session.read.json.assert_called_once_with("@stage_student_data/students.txt")

def test_get_missed_class_df(mock_session):
    expected_data = {"student_id": "123", "missed_days": 2}
    mock_session.read.json.return_value.select.return_value.flatten.return_value.select_expr.return_value = MagicMock()
    result = get_missed_class_df(mock_session)
    assert result is not None
    assert result == expected_data
    mock_session.read.json.assert_called_once_with("@stage_student_data/missed_classes.txt")

def test_main(mock_session):
    with pytest.mock.patch('your_module.get_student_df') as get_student_df_mock, \
         pytest.mock.patch('your_module.get_missed_class_df') as get_missed_class_df_mock:

        get_student_df_mock.return_value = {"student_id": "123", "name": "John", "english_grade": 90, "history_grade": 85, "math_grade": 95, "science_grade": 88}
        get_missed_class_df_mock.return_value = {"student_id": "123", "missed_days": 2}
        expected_result = "COMPLETED"
        result = main(mock_session)
        
        assert result == expected_result
