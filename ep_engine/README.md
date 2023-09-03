# Startup + warmup from initialised travel sample

ep_engine initialize -> create ep_bucket -> initialize ep_bucket + kv_bucket -> create vbucket map -> create shards -> start warmup -> start pager -> start bg fetcher

# Get Request

memcached get -> ep_engine -> kv bucket -> kv shard -> kv vbucket -> get
-> submit bg fetch if value not resident
