from sqlalchemy_dbtoolkit.engine.factory import AlchemyEngineFactory
from sqlalchemy_dbtoolkit.orm.base import ORMBaseManager
from sqlalchemy_dbtoolkit.query.create import InsertManager
from sqlalchemy_dbtoolkit.query.read import SelectManager
from sqlalchemy_dbtoolkit.utils.sanitization import sanitize_nan_to_none
from distance_matrix.database_models import Location, Distance


class DatabaseOperations:
    """
    Handles database engine creation, table definitions, and data insertion.
    """

    def __init__(self, dbms, db_name):
        """
        Initialize database connection and ORM base manager.

        Args:
            dbms (str): Database management system ('sqlite', 'mysql', 'postgresql').
            db_name (str): Name of the target database.
        """

        self.db_name = db_name
        self.dbms = dbms
        self.engine = self.get_engine()

        self.TableManager = ORMBaseManager(self.engine)

        self.Distance = Distance
        self.Location = Location

    def get_engine(self):
        """
        Create SQLAlchemy engine using provided DBMS and config.

        Returns:
            sqlalchemy.engine.Engine: SQLAlchemy engine object.

        Raises:
            ConnectionError: If engine creation fails.
        """

        try:
            DBEngine = AlchemyEngineFactory(dbms=self.dbms, db_name=self.db_name, config_path='../.config/config.ini')
            return DBEngine.engine
        except Exception as e:
            raise ConnectionError(f"Failed to create SQLAlchemy engine: {e}")

    def insert_data_in_distance_table(self, origin_name, destination_name, distance_km, duration_sec, timestamp_utc):
        """
        Insert a single row into the 'distances' table.

        Args:
            origin_name (str): Name of the origin.
            destination_name (str): Name of the destination.
            distance_km (int): Distance in kilometers.
            duration_sec (int): Duration in seconds.
            timestamp_utc (datetime): UTC timestamp of the measurement.

        Raises:
            ValueError: If insertion fails.
        """

        try:
            select_query = SelectManager(self.engine)
            origin = select_query.select_one_by_column(self.Location, "location_name", origin_name)
            destination = select_query.select_one_by_column(self.Location, "location_name", destination_name)

            insert_query = InsertManager(self.engine)
            data = {"origin_id": origin.id,
                    "destination_id": destination.id,
                    "distance_km": sanitize_nan_to_none(distance_km),
                    "duration_sec": sanitize_nan_to_none(duration_sec),
                    "timestamp_utc": timestamp_utc}
            insert_query.add_row(self.Distance, data)

        except Exception as e:
            raise ValueError(f"Failed to insert distance data: {e}")

    def insert_data_in_location_table(self, location_name, location_address):
        """
        Insert a single row into the 'locations' table.

        Args:
            location_name (str): Name of the location.
            location_address (str): Address of the location.

        Raises:
            ValueError: If insertion fails.
        """

        try:
            insert_query = InsertManager(self.engine)
            data = {"location_name": location_name,
                    "location_address": location_address}
            insert_query.add_row(self.Location, data)
        except Exception as e:
            raise ValueError(f"Failed to insert location data: {e}")

    def create_tables_if_not_exists(self):
        """
        Create database tables if they do not already exist.
        """

        try:
            self.TableManager.create_tables_if_not_exists()
        except Exception as e:
            raise RuntimeError(f"Failed to create database tables: {e}")
