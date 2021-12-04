from datetime import datetime, timedelta
from uuid import UUID

from peewee import *

#db = SqliteDatabase('accounts.db')
db = PostgresqlDatabase('parsing_db', user='devoinsta', password='sv2020kssv')


class BaseModel(Model):
    class Meta:
        database = db


class Account(BaseModel):
    login = CharField(max_length=64, unique=True)
    password = CharField(max_length=64, default='')
    settings = TextField(default='')
    limited_until = DateTimeField(default=datetime.now)
    parsed_today = IntegerField(default=0)
    locked_by = UUIDField(null=True)

    def try_lock(self, locked_by_uuid: UUID) -> bool:
        if not self.id or self.locked_by:
            return False

        model_class = type(self)
        query = model_class.update(locked_by=locked_by_uuid).where(
            (model_class.locked_by  == None) &
            (model_class.id == self.id)
        )
        locked = query.execute() != 0
        if locked:
            self.locked_by = locked_by_uuid

        return locked

    def try_unlock(self, locked_by_uuid: UUID) -> bool:
        if not self.id or not self.locked_by:
            return True

        model_class = type(self)
        query = model_class.update(locked_by=None).where(
            (model_class.locked_by == locked_by_uuid) &
            (model_class.id == self.id)
        )

        unlocked = query.execute() != 0
        if unlocked:
            self.locked_by = None

        return unlocked

    def set_limited(self):
        self.limited_until = datetime.now() + timedelta(hours=24)
        self.parsed_today = 0
        self.save()

    @classmethod
    def get_free_parsing_account(cls):
        return cls.select().where((cls.parsed_today < 25000) &
                                  (cls.limited_until < datetime.now()) &
                                  (cls.locked_by == None)).order_by(fn.Random()).get()


def create_tables():
    db.create_tables([Account])
