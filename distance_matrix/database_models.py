from sqlalchemy_dbtoolkit.orm.base import Base
from sqlalchemy import Column, Integer, String, TIMESTAMP, ForeignKey
from sqlalchemy.orm import relationship


class Location(Base):
    __tablename__ = 'locations'
    id = Column(Integer, primary_key=True)
    location_name = Column(String(length=255), nullable=False, unique=True)
    location_address = Column(String(length=255), nullable=False)

    origins = relationship("Distance", back_populates="origin", foreign_keys='Distance.origin_id')
    destinations = relationship("Distance", back_populates="destination", foreign_keys='Distance.destination_id')


class Distance(Base):
    __tablename__ = 'distances'
    id = Column(Integer, primary_key=True)
    origin_id = Column(Integer, ForeignKey('locations.id'), nullable=False)
    destination_id = Column(Integer, ForeignKey('locations.id'), nullable=False)
    distance_km = Column(Integer)
    duration_sec = Column(Integer)
    timestamp_utc = Column(TIMESTAMP(timezone=False))

    origin = relationship("Location", foreign_keys=[origin_id], back_populates="origins")
    destination = relationship("Location", foreign_keys=[destination_id], back_populates="destinations")