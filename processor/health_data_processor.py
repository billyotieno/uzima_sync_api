import os
import json
import pandas as pd


class HealthDataProcessor:
    def __init__(self, input_dir):
        self.input_dir = input_dir
        self.dataframes = []

    def process_files(self, user_id):
        """
        Process each JSON file in the extracted directory and associate with the provided user_id.
        """
        # Process each JSON file in the extracted directory
        for file_name in os.listdir(self.input_dir):
            if file_name.endswith('.json'):
                file_path = os.path.join(self.input_dir, file_name)
                # Use the passed user_id instead of extracting from file name
                self.process_file(file_path, user_id)

        # Combine all dataframes into one
        combined_df = pd.concat(self.dataframes, ignore_index=True)
        return combined_df

    def process_file(self, file_path, user_id):
        # Load the JSON file
        with open(file_path, 'r') as f:
            data = json.load(f)

        # Check if both workouts and metrics exist and process accordingly
        if 'workouts' in data['data'] and data['data']['workouts']:
            workout_data = self.flatten_workouts(data, user_id)
        else:
            workout_data = pd.DataFrame()  # Create an empty dataframe if no workouts are present

        if 'metrics' in data['data'] and data['data']['metrics']:
            metrics_data = self.flatten_metrics(data, user_id)
        else:
            metrics_data = pd.DataFrame()  # Create an empty dataframe if no metrics are present

        # Combine the flattened data and ensure both workouts and metrics exist
        if workout_data.empty:
            workout_data = pd.DataFrame(columns=['health_data_user', 'type', 'date', 'source', 'workout_qty',
                                                 'workout_units', 'elevation_qty', 'elevation_units', 'location'])

        if metrics_data.empty:
            metrics_data = pd.DataFrame(columns=['health_data_user', 'type', 'date', 'source', 'value',
                                                 'units', 'metric_name'])

        combined_df = pd.concat([workout_data, metrics_data], ignore_index=True)
        self.dataframes.append(combined_df)

        return combined_df


    def flatten_workouts(self, data, user_id):
        """ Flatten the workout data from the JSON file """
        flattened_workout_data = []
        for workout in data['data'].get('workouts', []):
            elevation = workout.get('elevationUp', {})
            for step in workout.get('stepCount', []):
                flattened_workout_data.append({
                    'health_data_user': user_id,
                    'type': 'workout',
                    'date': step.get('date'),
                    'source': step.get('source'),
                    'workout_qty': step.get('qty'),
                    'workout_units': step.get('units'),
                    'elevation_qty': elevation.get('qty', None),
                    'elevation_units': elevation.get('units', None),
                    'location': workout.get('location', None)
                })
        return pd.DataFrame(flattened_workout_data)


    def flatten_metrics(self, data, user_id):
        flattened_metrics_data = []
        for metric in data['data'].get('metrics', []):
            name = metric.get('name', None)
            units = metric.get('units', None)
            for entry in metric.get('data', []):
                flattened_metrics_data.append({
                    'health_data_user': user_id,
                    'type': 'metric',
                    'date': entry.get('date'),
                    'source': entry.get('source', None),
                    'value': entry.get('qty'),
                    'units': units,
                    'metric_name': name
                })
        return pd.DataFrame(flattened_metrics_data)
