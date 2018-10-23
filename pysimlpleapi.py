import os

from selectable import Selectable
from sqlite import Sqlite

class Api(object):
	def __init__(self, objs, root_dir, db_name):
		db_path = db_name
		if db_name != ":memory:":
			db_path = "{root}/{db_name}".format(root=root_dir, db_name=db_name) if root_dir else db_name
			if not os.path.exists(os.path.dirname(db_path)):
				os.makedirs(os.path.dirname(db_path))

		if not isinstance(objs, list):
			objs = [objs]
		self._objs = objs

		self._db_writer = Sqlite(database=db_path)
		self._db_writer.connect()

		self._db_reader = Sqlite(database=db_path)
		self._db_reader.connect()

		self.initialize()

	@property
	def db_writer(self):
		return self._db_writer

	@property
	def db_reader(self):
		return self._db_reader

	def initialize(self):
		for obj in self._objs:
			obj.create_table(db=self._db_writer)

	def save(self, obj):
		id_ = obj.save(self._db_writer)
		if obj.id < 0:
			obj.id = id_

	def delete(self, obj):
		stmt = Selectable().where(id=obj.id).delete(table_name=obj.table_name())
		return self._db_writer.delete(stmt)

	def create(self, obj, auto_save=True, **kwargs):
		inst = obj(**kwargs)
		if auto_save:
			self.save(inst)
		return inst

	def _get_many(self, selectable, table):
		return self._db_reader.select(selectable.select(table.table_name()), table)

	def _get_one(self, selectable, table):
		rows = self._db_reader.select(selectable.select(table.table_name()), table)
		return rows[0] if len(rows) > 0 else None
