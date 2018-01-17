
import pandas as pd
import sqlite3

import database

timeframes = ["2017-01", "2017-02", "2017-03"]

for timeframe in timeframes:
	connection = sqlite3.connect("{}.db".format(timeframe))
	c = connection.cursor()

	counter = 0
	last_unix = 0

	limit = 5000
	cur_length = limit
	test_done = False

	while cur_length == limit:
		sql = """
			SELECT * FROM parent_reply WHERE unix > {} AND parent_data NOT NULL 
			ORDER BY unix ASC LIMIT {}""".format(
				last_unix, limit)

		df = pd.read_sql(sql, connection)

		last_unix = df.tail(1)["unix"].values[0]
		cur_length = len(df)

		with open("training_data/train.from", "a", encoding="utf8") as f:
			for content in df["parent_data"].values:
				f.write(content+"\n")

		with open("training_data/train.to", "a", encoding="utf8") as f:
			for content in df["comment"].values:
				f.write(content+"\n")

		counter += 1
		if counter % 20 == 0:
			print(counter * limit, "rows completed so far")
