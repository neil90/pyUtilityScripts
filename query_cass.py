import ssl
import pandas as pd

from cassandra.cluster import Cluster

node_ip = #put node ip

cluster = Cluster([node_ip])

session = cluster.connect()

#Creates KEYSPACE
session.execute("""
				CREATE KEYSPACE twitter 
					WITH REPLICATION = { 'class' : 'SimpleStrategy' , 'replication_factor' : 1};
				""")

#Displays list of  Keyspaces(DB's) in Cassandra / use  link to see schema/cluster/list of tables etc(system views) 
#http://docs.datastax.com/en/cql/3.1/cql/cql_using/use_query_system_c.html
session.execute('USE SYSTEM')
dblist =  session.execute('SELECT keyspace_name FROM schema_keyspaces;')

#Puts List of Kespaces into Tabular form with pandas
fields = ['Keyspaces']
result = pd.DataFrame(list(dblist), columns = fields)

#Creates Table in twitter Keyspace
session.execute('USE twitter')
session.execute("""
					CREATE TABLE tweets (
						id int,
						screenname varchar,
						userurl varchar,
						created_at ascii,
						text varchar,
						timestampms timestamp,
						curr_timems timestamp,
						PRIMARY KEY (id, screenname)
						);
				""")

#Get some data and put in pandas
session.execute('USE twitter')
tweets = session.execute("""
				SELECT * FROM n_tweets LIMIT 5;
				""")

fields = ['id','text','tweettimestamp','userid','user_sn','user_url']
result = pd.DataFrame(list(tweets), columns = fields)
