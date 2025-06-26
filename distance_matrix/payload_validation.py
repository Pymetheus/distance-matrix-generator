import time
import datetime


def prepare_distance_matrix_api_payload(origins, destinations, mode=None, language=None, avoid=None, units=None,
                                        departure_time=None, arrival_time=None, transit_mode=None,
                                        transit_routing_preference=None, traffic_model=None, region=None):

    """
    Validate and assemble the Distance Matrix API request payload.

    Args:
        origins (list|str|tuple|dict): Origin locations.
        destinations (list|str|tuple|dict): Destination locations.
        mode (str): Mode of travel.
        language (str): Language code for results.
        avoid (str): Restrictions to avoid.
        units (str): Measurement units.
        departure_time (int|float|datetime|str): Departure time or "now".
        arrival_time (int|float|datetime): Arrival time.
        transit_mode (str): Preferred transit mode.
        transit_routing_preference (str): Transit routing preference.
        traffic_model (str): Traffic prediction model.
        region (str): Region biasing.

    Returns:
        dict: Validated and formatted config dictionary for API request.

    Raises:
        ValueError: If any parameter is invalid.
    """

    valid_modes = [None, "driving", "walking", "bicycling", "transit"]
    valid_avoids = [None, "tolls", "highways", "ferries"]
    valid_units = [None, "metric", "imperial"]
    valid_transit_modes = [None, "bus", "subway", "train", "tram", "rail"]
    valid_transit_routing_preferences = [None, "less_walking", "fewer_transfers"]
    valid_traffic_model = [None, "best_guess", "optimistic", "pessimistic"]

    if not is_valid_distance_matrix_query(origins):
        raise ValueError(f"Invalid origins: {origins}")

    if not is_valid_distance_matrix_query(destinations):
        raise ValueError(f"Invalid destinations: {destinations}")

    if mode not in valid_modes:
        raise ValueError(f"Invalid travel mode selection: {mode}")

    if avoid not in valid_avoids:
        raise ValueError(f"Invalid avoid selection: {avoid}")

    if units not in valid_units:
        raise ValueError(f"Invalid units selection: {units}")

    if departure_time and arrival_time:
        raise ValueError("Should not specify both departure_time and arrival_time.")
    else:
        if not is_valid_travel_time(departure_time):
            raise ValueError(f"Invalid departure_time: {departure_time}")
        elif not is_valid_travel_time(arrival_time):
            raise ValueError(f"Invalid arrival_time: {arrival_time}")

    if transit_mode not in valid_transit_modes:
        raise ValueError(f"Invalid transit mode selection: {transit_mode}")

    if transit_routing_preference not in valid_transit_routing_preferences:
        raise ValueError(f"Invalid transit routing preference: {transit_routing_preference}")

    if traffic_model not in valid_traffic_model:
        raise ValueError(f"Invalid traffic model selection: {traffic_model}")

    request_payload = {"origins": origins,
                     "destinations": destinations,
                     "mode": mode,
                     "language": language,
                     "avoid": avoid,
                     "units": units,
                     "departure_time": departure_time,
                     "arrival_time": arrival_time,
                     "transit_mode": transit_mode,
                     "transit_routing_preference": transit_routing_preference,
                     "traffic_model": traffic_model,
                     "region": region
                     }

    return request_payload


def is_valid_travel_time(travel_time):
    """
    Validate travel time input.

    Args:
        travel_time (int|float|datetime|str): Travel time value.

    Returns:
        bool: True if valid, False otherwise.
    """

    current_datetime = datetime.datetime.now()
    current_time = time.time()
    time_buffer_min = 4

    if isinstance(travel_time, str) and travel_time != "now":
        print(f"Invalid {type(travel_time)} - '{travel_time}' should be 'now' when string")
        return False

    elif isinstance(travel_time, datetime.datetime) and travel_time < (current_datetime - datetime.timedelta(minutes=time_buffer_min)):
        print(f"Invalid time {travel_time}, must be in future. Current time: {current_datetime} ")
        return False

    elif isinstance(travel_time, (int, float)) and travel_time < (current_time - (time_buffer_min*60)):
        print(f"Invalid time {travel_time}, must be in future. Current time: {current_time} ")
        return False
    else:
        return True


def is_valid_coordinate_dict(coordinate_dict):
    """
    Check if dictionary contains valid latitude and longitude.

    Args:
        coordinate_dict (dict): Dictionary with 'lat' and 'lng' keys.

    Returns:
        bool: True if valid coordinate dictionary, False otherwise.
    """

    valid_keys = ["lat", "lng"]
    if all(key in coordinate_dict for key in valid_keys):
        for key in valid_keys:
            if not is_valid_coordinate_pair([coordinate_dict[key]]):
                print(f"Wrong type for {key}: {coordinate_dict[key]} is {type(coordinate_dict[key]).__name__}")
                return False
        return True
    else:
        print(f"Missing one or more from valid keys ({valid_keys}) in dict: {coordinate_dict.keys()}")
        return False


def is_valid_coordinate_pair(coordinate_pair):
    """
    Check if coordinate pair contains numeric values.

    Args:
        coordinate_pair (list|tuple): Must contain exactly two numeric values - Latitude and longitude.

    Returns:
        bool: True if all values are float or int, False otherwise.
    """

    for coordinate in coordinate_pair:
        coordinate = convert_str_to_float(coordinate)
        if not isinstance(coordinate, (int, float)):
            print(f"Wrong type for {coordinate}: {type(coordinate).__name__}")
            return False
    return True


def convert_str_to_float(value):
    """
    Attempts to convert a string to a float, if applicable.

    Args:
        value (any): The value to convert. If it's a string representing a number,
                     it will be converted to a float. Other types are returned unchanged.

    Returns:
        any: The converted float if conversion is successful; otherwise, the original value.
    """
    try:
        if isinstance(value, str):
            value = float(value)
    except ValueError:
        print(f"Could not convert {value} to float: {type(value).__name__}")
    finally:
        return value


def is_valid_data_entry(data):
    """
    Validate an individual location data entry.

    Args:
        data (str|dict|tuple|list): Single data entry.

    Returns:
        bool: True if entry is valid, False otherwise.
    """

    if isinstance(data, str):
        if data[:3] == "ChI":
            print(f"Wrongly formatted PlaceID for {data}. Each Place ID string must be prepended with 'place_id:'")
            return False
        else:
            return bool(data.strip())  # returns False when string is empty or just consist of empty data
    elif isinstance(data, dict):
        return is_valid_coordinate_dict(data)
    elif isinstance(data, (list, tuple)):
        if len(data) == 2 and is_valid_coordinate_pair(data):
            return True
        else:
            print(f"Input {data} is not a valid coordinate pair")
            return False
    else:
        print(f"Input {data} is not a valid data entry: {type(data).__name__}")
        return False


def is_valid_distance_matrix_query(input_data):
    """
    Validate the structure and contents of origin or destination data.

    Args:
        input_data (str|dict|list|tuple): Location input data.

    Returns:
        bool: True if valid, False otherwise.
    """
    valid_types = (str, list, tuple, dict)

    if isinstance(input_data, valid_types):
        if isinstance(input_data, (list, tuple)):
            if isinstance(input_data, tuple) and len(input_data) == 2 and is_valid_coordinate_pair(input_data):
                return True  # coordinate pair
            elif len(input_data) == 0:
                print(f"Input {input_data} is empty: {len(input_data)}")
                return False
            else:
                return all(is_valid_data_entry(item) for item in input_data)
        elif isinstance(input_data, dict):
            return is_valid_coordinate_dict(input_data)
        else:
            return is_valid_data_entry(input_data)
    else:
        print(f"Input {input_data} is not a valid data entry: {type(input_data).__name__}")
        return False


def convert_data_to_list(data):
    """
    Convert input into a list format.

    Args:
        data (str|tuple|list): Input data.

    Returns:
        list: List-wrapped version of input.

    Raises:
        TypeError: If data is not convertible to list.
    """

    if isinstance(data, list):
        data = data
    elif isinstance(data, tuple):
        data = list(data)
    elif isinstance(data, str):
        data = [data]
    else:
        raise TypeError(f"Invalid data type: {type(data).__name__}")
    return data
