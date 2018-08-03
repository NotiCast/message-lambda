# pylint: disable=too-few-public-methods,missing-docstring
from sqlalchemy import (Column, Integer, Boolean, String, ForeignKey, Table,
                        UniqueConstraint)
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Client(Base):
    __tablename__ = "client"
    id = Column(Integer, primary_key=True)
    is_admin = Column(Boolean, nullable=False, default=False)
    users = relationship("User", backref="client", lazy=True)
    groups = relationship("Group", backref="client", lazy=True)
    devices = relationship("Device", backref="client", lazy=True)


class User(Base):
    __tablename__ = "user"
    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("client.id"),
                       nullable=False)
    username = Column(String(64), nullable=False, unique=True)
    password = Column(String(256), nullable=False)


devices2groups = Table(  # pylint: disable=invalid-name
    "devices2groups",
    Base.metadata,
    Column("device_id", Integer, ForeignKey("device.id"),
           primary_key=True, nullable=False),
    Column("group_id", Integer, ForeignKey("group.id"),
           primary_key=True, nullable=False)
)


class Device(Base):
    __tablename__ = "device"
    id = Column(Integer, primary_key=True)
    arn = Column(String(256), unique=True, nullable=False)
    name = Column(String(64), unique=False, nullable=False)
    client_id = Column(Integer, ForeignKey("client.id"),
                       nullable=False)
    __table_args__ = (UniqueConstraint("client_id", "name",
                                       name="_unique_group_name"),)


class Group(Base):
    __tablename__ = "group"
    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("client.id"),
                       nullable=False)
    arn = Column(String(256), nullable=False, unique=True)
    name = Column(String(64), nullable=False)
    devices = relationship("Device", secondary=devices2groups,
                           lazy="subquery", backref=backref("groups"))
    __table_args__ = (UniqueConstraint("client_id", "name",
                                       name="_unique_group_name"),)
    # devices exists in the `groups` backref
