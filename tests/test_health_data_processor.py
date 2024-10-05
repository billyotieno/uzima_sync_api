import unittest
from unittest.mock import patch, mock_open
import os
import json
import pandas as pd
from io import StringIO
from processor.health_data_processor import HealthDataProcessor


class TestHealthDataProcessor(unittest.TestCase):

    @patch('os.listdir')
    @patch('builtins.open', new_callable=mock_open)
    def test_process_files(self, mock_file, mock_listdir):
        # Mock file names
        mock_listdir.return_value = ['user1.json', 'user2.json']

        # Mock file content
        mock_file_data = json.dumps({
            "data": {
                "workouts": [{
                    "location": "park",
                    "elevationUp": {"qty": 50, "units": "meters"},
                    "stepCount": [{"date": "2024-10-01", "source": "watch", "qty": 1000, "units": "steps"}]
                }],
                "metrics": [{
                    "name": "heart_rate",
                    "units": "bpm",
                    "data": [{"date": "2024-10-01", "source": "watch", "qty": 70}]
                }]
            }
        })
        mock_file.return_value = StringIO(mock_file_data)

        processor = HealthDataProcessor(input_dir='mock_dir')
        result_df = processor.process_files()

        # Assert that the combined DataFrame has the correct shape
        self.assertEqual(result_df.shape, (2, 9))  # 2 rows, 9 columns for workouts + metrics

        # Assert that data is processed correctly
        expected_columns = ['health_data_user', 'type', 'date', 'source', 'workout_qty',
                            'workout_units', 'elevation_qty', 'elevation_units', 'location',
                            'value', 'units', 'metric_name']
        for column in expected_columns:
            self.assertIn(column, result_df.columns)

        self.assertEqual(result_df.iloc[0]['health_data_user'], 'user1')
        self.assertEqual(result_df.iloc[1]['metric_name'], 'heart_rate')

    @patch('builtins.open', new_callable=mock_open)
    def test_flatten_workouts(self, mock_file):
        # Mock JSON content for workouts
        mock_data = {
            "data": {
                "workouts": [{
                    "location": "park",
                    "elevationUp": {"qty": 50, "units": "meters"},
                    "stepCount": [{"date": "2024-10-01", "source": "watch", "qty": 1000, "units": "steps"}]
                }]
            }
        }
        processor = HealthDataProcessor(input_dir='mock_dir')
        df = processor.flatten_workouts(mock_data, 'user1')

        # Check DataFrame content
        self.assertEqual(df.shape, (1, 9))
        self.assertEqual(df.iloc[0]['location'], 'park')
        self.assertEqual(df.iloc[0]['workout_qty'], 1000)
        self.assertEqual(df.iloc[0]['elevation_units'], 'meters')

    @patch('builtins.open', new_callable=mock_open)
    def test_flatten_metrics(self, mock_file):
        # Mock JSON content for metrics
        mock_data = {
            "data": {
                "metrics": [{
                    "name": "heart_rate",
                    "units": "bpm",
                    "data": [{"date": "2024-10-01", "source": "watch", "qty": 70}]
                }]
            }
        }
        processor = HealthDataProcessor(input_dir='mock_dir')
        df = processor.flatten_metrics(mock_data, 'user1')

        # Check DataFrame content
        self.assertEqual(df.shape, (1, 7))
        self.assertEqual(df.iloc[0]['metric_name'], 'heart_rate')
        self.assertEqual(df.iloc[0]['value'], 70)
        self.assertEqual(df.iloc[0]['units'], 'bpm')

    @patch('builtins.open', new_callable=mock_open)
    def test_process_file_no_workouts_or_metrics(self, mock_file):
        # Mock JSON content without workouts and metrics
        mock_data = {
            "data": {
                "workouts": [],
                "metrics": []
            }
        }
        mock_file.return_value = StringIO(json.dumps(mock_data))

        processor = HealthDataProcessor(input_dir='mock_dir')

        # Call process_file and expect it to return an empty dataframe
        df = processor.process_file('mock_file.json', 'user1')

        # Check that the result is an empty DataFrame
        self.assertTrue(df.empty)


if __name__ == '__main__':
    unittest.main()