Get Request
memcached get -> ep_engine -> kv bucket -> kv shard -> kv vbucket -> get
-> submit bg fetch if value not resident
