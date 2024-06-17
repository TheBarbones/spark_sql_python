import datetime

# from pydantic_spark.base import SparkBase
from pydantic import BaseModel


class People(BaseModel):
    id: int
    name: str
    age: int
    phone_number: int
    birth_date: datetime.date


# class InitialObjects:
#     def __init__(self, entity):
#         self.entity = entity
#
#     def create_datarame(self):
#         pass
