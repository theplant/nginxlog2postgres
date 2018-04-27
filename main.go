package main

import (
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/jackc/pgx"
	"github.com/satyrius/gonx"
)

func main() {
	usage := `NGINX2P_LOG_FILE=./access.log NGINX2P_LOG_FORMAT="" NGINX2P_MAXLINES=100000 NGINX2P_TRUNCATE_TABLE=1 PGHOST=localhost PGUSER=sunfmin PGDATABASE=lacoste PGPASSWORD= nginxlog2postgres`
	logfile := os.Getenv("NGINX2P_LOG_FILE")
	logformat := os.Getenv("NGINX2P_LOG_FORMAT")
	maxlines := os.Getenv("NGINX2P_MAXLINES")

	truncate := os.Getenv("NGINX2P_TRUNCATE_TABLE")

	var maxlineCount int64
	var err error
	defer func() {
		if err != nil {
			fmt.Println("Usage:", usage)
		}
	}()
	if len(maxlines) > 0 {
		maxlineCount, err = strconv.ParseInt(maxlines, 10, 64)
		if err != nil {
			panic(err)
		}
	}

	if len(logformat) == 0 {
		logformat = `$http_x_forwarded_for $host - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent" $remote_addr $request_time $upstream_response_time`
	}

	logf, err := os.Open(logfile)
	if err != nil {
		panic(err)
	}
	reader := gonx.NewReader(logf, logformat)

	config, err := pgx.ParseEnvLibpq()
	if err != nil {
		panic(err)
	}

	rows := [][]interface{}{}

	fmt.Println("Start reading log file", logfile)

	i := 0
	for {
		i++
		var ent *gonx.Entry
		ent, err = reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		// fmt.Println(ent)

		status, err := ent.Field("status")
		if err != nil {
			panic(err)
		}

		statusInt, err := strconv.ParseInt(status, 10, 64)
		if err != nil {
			panic(err)
		}
		body_bytes_sent, err := ent.Field("body_bytes_sent")
		if err != nil {
			panic(err)
		}

		body_bytes_sent_int, err := strconv.ParseInt(body_bytes_sent, 10, 64)
		if err != nil {
			panic(err)
		}

		var upstream_response_time float64
		var strurt, _ = ent.Field("upstream_response_time")
		if strurt != "-" {
			upstream_response_time, err = ent.FloatField("upstream_response_time")
			if err != nil {
				panic(err)
			}
		}

		request_time, err := ent.FloatField("request_time")
		if err != nil {
			panic(err)
		}

		rows = append(rows, []interface{}{
			i,
			getString(ent, i, "request", 4096),
			statusInt,
			body_bytes_sent_int,
			getString(ent, i, "remote_addr", 255),
			upstream_response_time,
			getString(ent, i, "http_x_forwarded_for", 255),
			getString(ent, i, "host", 255),
			getString(ent, i, "remote_user", 255),
			getString(ent, i, "time_local", 255),
			getString(ent, i, "http_referer", 4096),
			getString(ent, i, "http_user_agent", 4096),
			request_time,
		})

		if i%10000 == 0 {
			fmt.Printf("%d read\n", i)
		}
		if maxlineCount > 0 && int64(i) >= maxlineCount {
			break
		}
	}

	fmt.Printf("Finished read: %d rows\n", len(rows))

	var conn *pgx.Conn

	conn, err = pgx.Connect(config)
	if err != nil {
		panic(err)
	}

	conn.SetLogLevel(pgx.LogLevelTrace)

	fmt.Println("Check OR Creating table nginx_access_logs...")
	_, err = conn.Exec(`
CREATE TABLE IF NOT EXISTS nginx_access_logs
(
	line_no                BIGINT,
	request                VARCHAR(4096),
	status                 BIGINT,
	body_bytes_sent        BIGINT,
	remote_addr            VARCHAR(255),
	upstream_response_time DOUBLE PRECISION,
	http_x_forwarded_for   VARCHAR(255),
	host                   VARCHAR(255),
	remote_user            VARCHAR(255),
	time_local             VARCHAR(255),
	http_referer           VARCHAR(4096),
	http_user_agent        VARCHAR(4096),
	request_time           DOUBLE PRECISION
);
		`)
	if err != nil {
		panic(err)
	}
	// fmt.Println(rows)
	yes, _ := strconv.ParseBool(truncate)
	if yes {
		_, err = conn.Exec(`TRUNCATE nginx_access_logs`)
		if err != nil {
			panic(err)
		}
	}

	copyCount, err := conn.CopyFrom(
		pgx.Identifier{"nginx_access_logs"},
		[]string{
			"line_no",
			"request",
			"status",
			"body_bytes_sent",
			"remote_addr",
			"upstream_response_time",
			"http_x_forwarded_for",
			"host",
			"remote_user",
			"time_local",
			"http_referer",
			"http_user_agent",
			"request_time",
		},
		pgx.CopyFromRows(rows),
	)

	if err != nil {
		panic(err)
	}

	fmt.Printf("Rows %d inserted\n", copyCount)

}

func getString(ent *gonx.Entry, line int, field string, maxLength int) (value string) {
	var err error
	value, err = ent.Field(field)
	if err != nil {
		panic(err)
	}
	checkLength(line, field, value, maxLength)
	return
}

func checkLength(line int, field, value string, length int) {
	if len(value) > length {
		panic(fmt.Sprintf("Line %d, Field: %s, Value too long (%d): %s", line, field, length, value))
	}
}
