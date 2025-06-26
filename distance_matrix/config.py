import os
import configparser


class Config(object):
    """
    Loads configuration from INI file.
    """

    def __init__(self, config_path='../.config/config.ini'):
        """
        Initialize Config with the path to the INI file.
        Args:
            config_path (str): Path to the configuration file.
        """

        self.config_path = config_path
        self.config = self.import_config()

    def validate_config_path(self):
        """
        Raise error if config path does not exist.
        """

        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found in: {self.config_path}")

    def validate_file_readability(self, readable_file):
        """
        Raise error config file is not readable.
        """

        if not readable_file:
            raise ValueError(f"Failed to read config file at: {self.config_path}")

    def validate_file_integrity(self, configuration):
        """
        Raise error if config file contains no sections.
        """

        if not configuration.sections():
            raise ValueError(f"Config file contains no sections: {self.config_path}")

    def import_config(self):
        """
        Validate, parse and return the configuration object.

        Returns:
            ConfigParser: Parsed configuration object.

        Raises:
            ValueError: If any step in reading or validating the config fails.
        """

        try:
            self.validate_config_path()
            config = configparser.ConfigParser(interpolation=None)
            file_reader = config.read(self.config_path)
            self.validate_file_readability(file_reader)
            self.validate_file_integrity(config)
            return config
        except Exception as e:
            raise ValueError(f"Failed to import config file: {e}")

    @property
    def api_key(self):
        """
        Return api key.
        """

        try:
            return self.config["google"]["api_key"]
        except KeyError:
            raise KeyError("Missing 'api_key' under [google] section.")
