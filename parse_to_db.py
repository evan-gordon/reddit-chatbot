import sqlite3, json
from datetime import datetime

TIME_TO_PARSE = '2018-08'

sql_batch = []

connection = sqlite3.connect(f'{TIME_TO_PARSE}.db')
conn = connection.cursor()

def create_table():
  conn.execute(
      """
  CREATE TABLE IF NOT EXISTS parent_reply(
    parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, 
    comment TEXT, subreddit TEXT, unix INT, score INT
  )
  """
  )

def format_data(data: any):
  patterns = [("\n", " new_line "), ("\r", " new_line "), ('"', "'")]
  for p in patterns:
    data = data.replace(p[0], p[1])
  return data

def query_comment_parents(field: str, parent_id: str):
  try:
    query = f"SELECT {field} FROM parent_reply WHERE comment_id = '{parent_id}' LIMIT 1"
    conn.execute(query)
    result = conn.fetchone()
    if result != None:
      return result[0]
    else:
      return None
  except Exception as e:
    return None

def acceptable_comment(data):
  if (
      len(data.split(' ')) > 50 or len(data) < 1 or len(data) > 1000 or
      data == '[deleted]' or data == '[removed]'
  ):
    return False
  return True

def transaction_bldr(query):
  global sql_batch
  sql_batch.append(query)
  if (len(sql_batch) > 1000):
    conn.execute('BEGIN TRANSACTION')
    for b in sql_batch:
      try:
        conn.execute(b)
      except Exception as e:
        pass
        # print(f"ERROR:: {str(e)}, \n{query}")
    # print(f"failed: {failed} out of {len(sql_batch)}")
    # exit()
    connection.commit()
    sql_batch = []

def sql_insert_replace_comment(
    comment_id, parent_id, parent, comment, sub, time, score
):
  try:
    query = f"""
    UPDATE parent_reply
    SET parent_id = {parent_id}, comment_id = {comment_id}, parent = {parent}, comment = {comment}, 
    subreddit = {sub}, unix = {int(time)}, score = {score} 
    WHERE parent_id = {parent_id};
    """
    transaction_bldr(query)
  except Exception as e:
    print('replace_comment_error', str(e))

def sql_insert_with_parent(
    comment_id, parent_id, parent, comment, sub, time, score
):
  try:
    query = f"""
    INSERT INTO parent_reply(parent_id, comment_id, parent, comment, subreddit, unix, score)
    VALUES ("{parent_id}", "{comment_id}", "{parent}", "{comment}", "{sub}", {int(time)}, {score});
    """
    transaction_bldr(query)
  except Exception as e:
    print('insert_w_parent_comment_error', str(e))

def sql_insert_no_parent(comment_id, parent_id, comment, sub, time, score):
  try:
    query = f"""
    INSERT INTO parent_reply(parent_id, comment_id, comment, subreddit, unix, score)
    VALUES ("{parent_id}", "{comment_id}", "{comment}", "{sub}", {int(time)}, {score});
    """
    transaction_bldr(query)
  except Exception as e:
    print('insert_comment_error', str(e))

if __name__ == "__main__":
  create_table()
  curr_row = 0
  paired_rows = 0

  file_path = f"data/RC_{TIME_TO_PARSE}"
  banned_subs = ["sweden", "greece"]
  with open(file_path, 'r', buffering=1000) as buff:
    for row in buff:
      try:
        curr_row += 1
        row = json.loads(row)
        comment_id, parent_id = row['id'], row['parent_id'].split('_')[1]
        body = format_data(row['body'])
        created_utc = row['created_utc']
        score = row['score']
        sub = row['subreddit']
        # only use good comments
        if (
            score >= 2 and sub not in banned_subs and
            acceptable_comment(body)
        ):
          parent = query_comment_parents('comment', parent_id)
          ex_score = query_comment_parents('score', parent_id)
          if (ex_score and score > ex_score):
            sql_insert_replace_comment(
                comment_id, parent_id, parent, body, sub, created_utc,
                score
            )
          elif (parent):
            sql_insert_with_parent(
                comment_id, parent_id, parent, body, sub, created_utc,
                score
            )
            paired_rows += 1
          else:
            sql_insert_no_parent(
                comment_id, parent_id, body, sub, created_utc, score
            )
        if (curr_row % 10000 == 0):
          d = conn.execute("SELECT count(*) FROM parent_reply").fetchone()
          print(
              f"Read rows: {curr_row}, Paired rows: {paired_rows}, Rows Inserted: {d[0]}, Time: {str(datetime.now())}"
          )
      except Exception as e:
        pass
