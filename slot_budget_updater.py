import mysql.connector

conn = mysql.connector.connect(user='admin', password='password',
                              host='ads-database.czdhajbrjw1o.us-east-1.rds.amazonaws.com',database='ads_db')
cur=conn.cursor()
cur.execute("update ads set current_slot_budget= budget/((budget/current_slot_budget)-1)")
for row in cur:
    print(row)
conn.commit()
cur.close()
conn.close()
