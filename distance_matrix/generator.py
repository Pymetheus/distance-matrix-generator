import pandas as pd
import numpy as np
import os
import json
from distance_matrix.database_handler import DatabaseOperations


class DistanceMatrixGenerator:
    """
        Generates a distance matrix using previously fetched Google Maps Distance Matrix API data.

        Responsibilities include:
        - Loading and validating the raw API response from a JSON file.
        - Mapping origin and destination names to their corresponding addresses.
        - Building a pandas DataFrame of distances and exporting it as CSV.
        - Optionally inserting the data into a database.

        Args:
            filename (str): The base name of the JSON file containing the API response.
            origin_names (list[str]): List of human-readable origin labels.
            destination_names (list[str]): List of human-readable destination labels.
            write_to_db (bool): Whether to write the distance and location data to a database.
            dbms (str): Database system identifier ('sqlite', 'mysql', 'postgresql').
            db_name (str): Target database name.
        """

    def __init__(self, filename, origin_names, destination_names, write_to_db=True, dbms='sqlite', db_name='test_distance_database'):
        self.filename = filename
        self.origin_names = self.sanitize_labels(origin_names)
        self.destination_names = self.sanitize_labels(destination_names)

        self.write_to_db = write_to_db

        try:
            if self.write_to_db:
                self.DB_Ops = DatabaseOperations(dbms=dbms, db_name=db_name)
                self.DB_Ops.create_tables()
        except Exception as e:
            print(f"WARNING: Could not connect to database: {e}")
            self.write_to_db = False

        self.raw_api_response = None
        self.origin_addresses = None
        self.destination_addresses = None
        self.distance_matrix_df = None

        self.load_raw_api_response()

    def load_raw_api_response(self):
        """
        Loads the raw API response from a JSON file and extracts the origin and destination addresses.

        Raises:
            ValueError: If the file cannot be loaded or parsed.
        """

        try:
            filepath = f"../data/raw/{self.filename}.json"

            with open(filepath, "r") as file:
                self.raw_api_response = json.load(file)
                self.origin_addresses = self.raw_api_response['origin_addresses']  # used as rows
                self.destination_addresses = self.raw_api_response['destination_addresses']  # used as columns
        except Exception as e:
            raise ValueError(f"Failed to load fetched data: {e}")

    def validate_api_response(self):
        """
        Validates that the structure of the API response matches the expected format.

        Ensures that the number of rows matches the number of origins and
        that each row has the correct number of destination elements.

        Raises:
            ValueError: If the structure is inconsistent or malformed.
        """

        try:
            if len(self.raw_api_response['rows']) != len(self.origin_addresses):
                raise ValueError("Mismatch between origin addresses and rows in fetched API data")

            for row_index, row in enumerate(self.raw_api_response['rows']):
                if len(row['elements']) != len(self.destination_addresses):
                    raise ValueError(f"Row {row_index} has incorrect number of elements")
        except Exception as e:
            raise ValueError(f"Failed to validate fetched data: {e}")

    def insert_locations_if_not_exists(self, names, addresses):
        """
        Inserts location records into the database if they do not already exist.

        Args:
            names (list[str]): List of location names.
            addresses (list[str]): Corresponding list of location addresses.
        """

        for name, address in zip(names, addresses):
            try:
                self.DB_Ops.insert_data_in_location_table(name, address)
            except Exception as e:
                print(f"WARNING: {name} already in locations table: {e}")

    def build_matrix(self):
        """
        Constructs a distance matrix DataFrame from the API response.

        - Validates the raw response.
        - Populates a DataFrame with distances between all origin-destination pairs.
        - Optionally inserts both location and distance data into the database.
        - Exports the matrix as a CSV file.

        Raises:
            ValueError: If any part of the matrix-building process fails.
        """

        try:
            self.validate_api_response()
            self.distance_matrix_df = pd.DataFrame(data=self.origin_names, columns=["Matrix"])
            self.distance_matrix_df.set_index("Matrix", inplace=True)

            if self.write_to_db:
                self.insert_locations_if_not_exists(self.origin_names, self.origin_addresses)
                self.insert_locations_if_not_exists(self.destination_names, self.destination_addresses)

            timestamp_utc = self.raw_api_response["request_time_iso"]

            for column, column_name in enumerate(self.destination_addresses):
                for row, row_name in enumerate(self.origin_addresses):

                    element_data = self.raw_api_response['rows'][row]['elements'][column]
                    distance_data = self.extract_travel_attribute(element_data, 'distance')
                    # print(f"Direction from '{row_name}' to '{column_name}' is: {distance_data}")

                    row_alias = self.origin_names[row]
                    column_alias = self.destination_names[column]
                    self.distance_matrix_df.loc[row_alias, column_alias] = distance_data

                    duration_data = self.extract_travel_attribute(element_data, 'duration')
                    # print(f"Duration from '{row_name}' to '{column_name}' is: {duration_data}")

                    if self.write_to_db:
                        self.DB_Ops.insert_data_in_distance_table(origin_name=row_alias,
                                                                  destination_name=column_alias,
                                                                  distance_km=distance_data,
                                                                  duration_sec=duration_data,
                                                                  timestamp_utc=timestamp_utc)

            self.export_matrix_as_csv(self.distance_matrix_df)

        except Exception as e:
            raise ValueError(f"Failed to generate distance matrix: {e}")

    def extract_travel_attribute(self, element_data, attribute):
        """
        Extracts a travel attribute from a single API response element.

        Converts distances to kilometers and durations to seconds. Returns NaN for missing or failed results.

        Args:
            element_data (dict): The 'element' block from the API response.
            attribute (str): Either "distance" or "duration".

        Returns:
            int | float: Value of the specified attribute, or np.nan if unavailable.

        Raises:
            ValueError: If an unsupported attribute is requested.
        """

        request_status = element_data['status']

        if request_status == 'OK':
            if attribute not in ("distance", "duration"):
                raise ValueError(f"Unsupported data method: {attribute}")

            result = element_data[attribute]['value']
            if attribute == 'distance':
                km_result = int(round((result / 1000), ndigits=0))
                return km_result
            elif attribute == 'duration':
                return result

        elif request_status == "ZERO_RESULTS":
            # print(f"Direction calculation is not possible see status: {request_status}")
            return np.nan
        elif request_status == "NOT_FOUND":
            # print(f"Origin or destination not found see status: {request_status}")
            return np.nan
        else:
            # print(f"Error for origin or destination see status: {request_status}")
            return np.nan

    def sanitize_labels(self, labels):
        """
        Cleans and standardizes a list of location labels.
        Converts all labels to title case ,replaces empty, whitespace-only, or non-string entries with 'Unknown'.

        Args:
            labels (list): List of label strings (e.g., origin or destination names).

        Returns:
            list[str]: Sanitized list of label strings.
        """

        sanitized_labels = labels.copy()

        for index, label in enumerate(sanitized_labels):
            if not isinstance(label, str):
                label = "Unknown"
            clean_label = label.strip().lower().title()
            if not bool(clean_label.strip()):
                clean_label = "Unknown"
            sanitized_labels[index] = clean_label
        return sanitized_labels

    def export_matrix_as_csv(self, matrix, directory='../data/processed/'):
        """
        Exports the computed distance matrix as a CSV file.

        Args:
            matrix (pd.DataFrame): The completed distance matrix.
            directory (str): Directory where the CSV will be saved.

        Raises:
            ValueError: If writing the file fails.
        """

        try:
            export_path = os.path.join(directory, f"{self.filename}.csv")
            matrix.to_csv(export_path, na_rep="NaN")
            print(f"Data Frame has been written to {export_path}")
        except Exception as e:
            raise ValueError(f"Failed to export distance matrix csv: {e}")
