# Import nginx log into postgres table

Install

```
go get -v github.com/theplant/nginxlog2postgres
```

Run

```
NGINX2P_LOG_FILE=./access.log NGINX2P_LOG_FORMAT="" NGINX2P_MAXLINES=100000 NGINX2P_TRUNCATE_TABLE=1 PGHOST=localhost PGUSER=sunfmin PGDATABASE=lacoste PGPASSWORD= nginxlog2postgres
```
