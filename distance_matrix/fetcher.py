import googlemaps
import json
import os
import re
import hashlib
import unicodedata
from datetime import datetime, timezone
from distance_matrix.config import Config
from distance_matrix.payload_validation import prepare_distance_matrix_api_payload, convert_data_to_list


class GoogleMapsFetcher:
    """
    Fetch and store Google Maps Distance Matrix API responses.
    """

    def __init__(self, origins_query, destinations_query):
        """
        Initialize fetcher with origin and destination queries.

        Args:
            origins_query (str|list|tuple): Origin location(s).
            destinations_query (str|list|tuple): Destination location(s).
        """

        self.api_payload = prepare_distance_matrix_api_payload(origins_query, destinations_query)
        self.search_query = convert_data_to_list(origins_query) + convert_data_to_list(destinations_query)

        self.client = googlemaps.Client(key=Config().api_key)
        self.api_response = None
        self.filename = None

    def fetch_distance_matrix(self):
        """
        Send request to Distance Matrix API and store response.

        Raises:
            ValueError: If API call fails or response is invalid.
        """
        try:
            api_response = self.client.distance_matrix(**self.api_payload)

            if api_response.get("status") == "OK":
                self.api_response = api_response
                api_label = "dist_matrix_data"
                self.build_filename_from_query(api_label)
            else:
                raise ValueError(f"Failed to fetch data: {api_response['status']}")
        except Exception as e:
            raise ValueError(f"Error fetching distance matrix: {e}")

    def append_request_timestamp(self):
        """
        Add UTC ISO timestamp to the API response.
        """

        self.api_response["request_time_iso"] = datetime.now(timezone.utc).isoformat()

    def run_fetch_pipeline(self):
        """
        Execute full API fetch pipeline: validate, request, timestamp, archive.

        Raises:
            ValueError: If any step in the pipeline fails.
        """
        try:
            prepare_distance_matrix_api_payload(**self.api_payload)  # Revalidate config before API call
            self.fetch_distance_matrix()
            self.append_request_timestamp()
            self.archive_api_response()

        except Exception as e:
            raise ValueError(f"Failed to execute query: {e}")

    def sanitize_query(self, search_query, max_length=6):
        """
        Clean search query string to safe filename fragment.

        Args:
            search_query (str): Query input to sanitize.
            max_length (int): Maximum length of returned string.

        Returns:
            str: Sanitized string.
        """

        clean_query = unicodedata.normalize('NFKD', str(search_query)).encode('ascii', 'ignore').decode('ascii')
        clean_query = re.sub(r'\W+', '', clean_query)
        clean_query = clean_query[:max_length]
        return clean_query

    def generate_hash_code(self, data, max_length=7):
        """
        Generate short MD5 hash from string.

        Args:
            data (str): Input to hash.
            max_length (int): Length of hash slice.

        Returns:
            str: Truncated hash string.
        """

        hash_code = hashlib.md5(data.encode()).hexdigest()[:max_length]
        return hash_code

    def build_filename_from_query(self, label):
        """
        Construct filename using search query and hash.

        Args:
            label (str): Label to prefix in filename.

        Raises:
            ValueError: If search_query is invalid type.
        """

        if isinstance(self.search_query, (tuple, list)):
            query_suffix = "_".join(self.sanitize_query(item, max_length=6) for item in self.search_query[:3])
        elif isinstance(self.search_query, str):
            query_suffix = self.sanitize_query(self.search_query)
        else:
            raise ValueError("Unsupported search_query type: must be str or list[str]")

        hash_code = self.generate_hash_code(str(self.search_query))
        self.filename = f"gmaps_{label}_{query_suffix}_{hash_code}"

    def archive_api_response(self, directory='../data/raw/'):
        """
        Save API response to local JSON file.

        Args:
            directory (str): Directory path to save the file.

        Raises:
            ValueError: If response is missing or file write fails.
        """

        try:
            filepath = os.path.join(directory, f"{self.filename}.json")
            if not self.api_response:
                raise ValueError("No valid API response to archive.")

            with open(filepath, 'w') as file:
                json.dump(self.api_response, file, indent=4)
            print(f"Saved API response to {filepath}")
        except Exception as e:
            raise ValueError(f"Failed to archive api response: {e}")
