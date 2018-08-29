import logging

from sqlalchemy.sql import and_
from sqlalchemy.exc import OperationalError


log = logging.getLogger(__name__)


def _clauses(table, **kwargs):
    return (getattr(table.c, name) == value for name, value in list(kwargs.items()))


class DB(object):

    def __init__(self, engine):
        self.engine = engine

    def execute(self, *args, **kwargs):
        def _get():
            with self.engine.begin() as conn:
                return conn.execute(*args, **kwargs)
        try:
            return _get()
        # try harder to prevent MySQL "server has gone away" error:
        except OperationalError as e:
            log.warn(str(e))
            return _get()

    def get(self, table, *fields, **kwargs):
        result = table.select()
        if fields:
            result = result.with_only_columns(fields)
        return self.execute(result.where(and_(_clauses(table, **kwargs))))

    def create(self, table, **kwargs):
        res = self.execute(table.insert().values(**kwargs))
        return res.inserted_primary_key[0]

    def update(self, table, id, **kwargs):
        return self.execute(
            table.update().where(and_(_clauses(table, id=id))).values(**kwargs))

    def delete(self, table, **kwargs):
        return self.execute(
            table.delete().where(and_(_clauses(table, **kwargs))))
