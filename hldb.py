#!/usr/bin/env python3
from peewee import Model, TextField, IntegerField, TimestampField
from playhouse.apsw_ext import APSWDatabase

db = APSWDatabase('hlcf.db')


def db_connect(name):
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    database = APSWDatabase(name)
    database.init(name, timeout=60, pragmas=(
        ('journal_mode', 'wal'),
        ('page_size', 4096),
        ('cache_size', -1024 * 64),
        ('temp_store', 'memory'),
        ('synchronous', 'off')))
    database.connect()
    database.create_tables([Directories, Files])
    return database


class BaseModel(Model):
    class Meta:
        database = db


class Directories(BaseModel):
    class Meta:
        table_name = 'directories'

    path = TextField(default=None, null=True, unique=True)
    date = TimestampField(default=None, null=True)
    scraped = TimestampField(default=None, null=True)
    visited = TimestampField(default=None, null=True)


class Files(BaseModel):
    class Meta:
        table_name = 'files'

    path = TextField(default=None, null=True, unique=True)
    size = IntegerField(default=0, null=True)
    date = TimestampField(default=None, null=True)
    scraped = TimestampField(default=None, null=True)
    visited = TimestampField(default=None, null=True)
