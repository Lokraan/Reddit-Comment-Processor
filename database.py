
from datetime import datetime
import sqlite3
import json
import os

# 24300000, 24900000, 24500000 

FILE = "D:/2017/RC_2017-01"
TIME_FRAME = "2017-01"

class Database:
	def __init__(self, name):
		self._connection = sqlite3.connect('{}.db'.format(name))
		self._c = self._connection.cursor()


class CommentDatabase(Database):
	def __init__(self, name, start=0, paired_comments=0, score_threshold=2, vacuum_interval=100000, output_interval=None):
		super().__init__(name)

		self._start = start
		self._counter = start
		self._paired_comments = paired_comments
		self._score_threshold = score_threshold
		self._output_interval = output_interval
		self._vacuum_interval = vacuum_interval

		self._create_table()

		self._sql_transaction = set()


	def _create_table(self):
		self._c.execute(
			"""
			CREATE TABLE IF NOT EXISTS parent_reply 
			(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent_data TEXT, 
			comment TEXT, subreddit TEXT, unix INT, score INT)
			"""
		)


	def _format_data(self, data):
		data = data.replace(
			"\n", " newlinechar "

			).replace(
			"\r", " newlinechar "

			).replace(
			'"', "'"
			)

		return data


	def _sql_query(self, query):
		self._c.execute(query)
		result = self._c.fetchone()
		if result:
			return result[0]

		return None


	def _transaction_bldr(self, sql):
		self._sql_transaction.add(sql)

		if len(self._sql_transaction) > self._output_interval:
			self._c.execute("BEGIN TRANSACTION")
			for s in self._sql_transaction:
				try:
					self._c.execute(s)
				except sqlite3.IntegrityError:
					pass

			self._connection.commit()
			self._sql_transaction.clear()


	def _acceptable(self, data):
		if not data:
			return False

		elif len(data.split(' ')) > 50 or len(data) == 0 or len(data) > 1000:
			return False

		elif data == "[deleted]" or data == "[removed]":
			return False

		return True


	def _find_existing_score(self, parent_id):
		sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(
			parent_id)

		return self._sql_query(sql)


	def _find_parent(self, parent_id):
		sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(
			parent_id)

		return self._sql_query(sql)


	def _replace_comment(self, parent_id, comment_id, parent_data, body, subreddit, created_utc, score):
		sql = """
		UPDATE parent_reply SET parent_id = "{}", comment_id = "{}", parent_data = "{}", 
		comment = "{}",subreddit = "{}", unix = {}, score = "{}"
		WHERE parent_id = "{}";""".format(
			 parent_id, comment_id, parent_data, body, 
			 subreddit, int(created_utc), score, parent_id
			 )

		self._transaction_bldr(sql)


	def _has_parent(self, parent_id, comment_id, parent_data, body, subreddit, created_utc, score):
		sql = """
		INSERT INTO parent_reply (parent_id, comment_id, parent_data, comment, subreddit, unix, score) 
		VALUES ("{}", "{}", "{}", "{}", "{}", {}, {});""".format(
			parent_id, comment_id, parent_data, body, subreddit, int(created_utc), score
			)

		self._transaction_bldr(sql)


	def _no_parent(self, parent_id, comment_id, body, subreddit, created_utc, score):
		sql = """
		INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score)
		VALUES ("{}", "{}", "{}", "{}", {}, {});""".format(
			parent_id, comment_id, body, subreddit, int(created_utc), score
			)

		self._transaction_bldr(sql)


	def _start_vacuum(self):
		self._c.execute("VACUUM")
		self._connection.commit()


	def _start_cleanup(self):
		sql = "DELETE FROM parent_reply WHERE parent_data IS NULL"
		self._c.execute(sql)
		self._connection.commit()
		# num_rows = self._sql_query("SELECT Count(*) FROM parent_reply")
		# print("Cleaned up | Time: {} | Num Rows {}".format(str(datetime.now()), num_rows))


	def handle_comment(self, comment):
		self._counter += 1

		comment_id = comment["id"]
		parent_id = comment["parent_id"].split('_')[1]
		body = self._format_data(comment["body"])
		created_utc = comment["created_utc"]
		score = comment["score"]
		subreddit = comment["subreddit"]

		if self._acceptable(body):
			if score >= self._score_threshold:
				existing_comment_score = self._find_existing_score(parent_id)
				parent_data = self._find_parent(parent_id)
				
				if existing_comment_score and score > existing_comment_score:
					self._replace_comment(parent_id, comment_id, parent_data, body, subreddit, created_utc, score)

				elif parent_data:
					self._has_parent(parent_id, comment_id, parent_data, body, subreddit, created_utc, score)
					self._paired_comments += 1

				else:
					self._no_parent(parent_id, comment_id, body, subreddit, created_utc, score)


		if self._output_interval and self._counter % self._output_interval == 0:
			print("Comments Read: {} | Paired Comments: {} | Time: {}".format(
				self._counter, self._paired_comments, str(datetime.now())))

		if self._vacuum_interval and self._counter % self._vacuum_interval == 0:
			self._start_vacuum()


if __name__ == '__main__':
	with open(FILE) as f:
		db = CommentDatabase(name=TIME_FRAME, output_interval=10000000, vacuum_interval=100000000)
		for row in f:
			row = json.loads(row)
			db.handle_comment(row)

		db._start_cleanup()
