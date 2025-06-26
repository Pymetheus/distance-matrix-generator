from sqlalchemy_dbtoolkit.engine.factory import AlchemyEngineFactory
from sqlalchemy_dbtoolkit.orm.base import ORMBaseManager
from sqlalchemy_dbtoolkit.query.create import InsertManager
from sqlalchemy_dbtoolkit.query.read import SelectManager
from sqlalchemy_dbtoolkit.utils.sanitization import sanitize_nan_to_none
from sqlalchemy import Column, Integer, String, TIMESTAMP, ForeignKey
from sqlalchemy.orm import relationship


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
        self.Base = self.TableManager.Base

        self.Distance = None
        self.Location = None

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

    def create_tables(self):
        """
        Define and create the 'locations' and 'distances' tables in the database.

        Raises:
            RuntimeError: If table creation fails.
        """

        try:

            class Location(self.Base):
                __tablename__ = 'locations'
                id = Column(Integer, primary_key=True)
                location_name = Column(String(length=255), nullable=False, unique=True)
                location_address = Column(String(length=255), nullable=False)

                origins = relationship("Distance", back_populates="origin", foreign_keys='Distance.origin_id')
                destinations = relationship("Distance", back_populates="destination", foreign_keys='Distance.destination_id')

            class Distance(self.Base):
                __tablename__ = 'distances'
                id = Column(Integer, primary_key=True)
                origin_id = Column(Integer, ForeignKey('locations.id'), nullable=False)
                destination_id = Column(Integer, ForeignKey('locations.id'), nullable=False)
                distance_km = Column(Integer)
                duration_sec = Column(Integer)
                timestamp_utc = Column(TIMESTAMP(timezone=False))

                origin = relationship("Location", foreign_keys=[origin_id], back_populates="origins")
                destination = relationship("Location", foreign_keys=[destination_id], back_populates="destinations")

            self.Distance = Distance
            self.Location = Location
            self.TableManager.create_tables()

        except Exception as e:
            raise RuntimeError(f"Failed to create database tables: {e}")
