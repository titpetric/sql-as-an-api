package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"io/ioutil"
	"net/http"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-chi/chi"
	"github.com/titpetric/factory"
	"github.com/titpetric/factory/resputil"
)

func handleError(err error, message string) {
	if message == "" {
		message = "Error making API call"
	}
	if err != nil {
		log.Fatalf(message+": %v", err.Error())
	}
}

func main() {
	// set up flags
	var (
		addr = flag.String("addr", ":3000", "Listen address for HTTP server")
	)
	flag.Parse()

	// log to stdout not stderr
	log.SetOutput(os.Stdout)

	// set up database connection
	factory.Database.Add("default", "sqlapi:sqlapi@tcp(db1:3306)/sqlapi?collation=utf8mb4_general_ci")

	db, err := factory.Database.Get()
	handleError(err, "Can't connect to database")
	db.Profiler = &factory.Database.ProfilerStdout

	// listen socket for http server
	log.Println("Starting http server on address " + *addr)
	listener, err := net.Listen("tcp", *addr)
	handleError(err, "Can't listen on addr "+*addr)

	r := chi.NewRouter()
	r.Get("/api/{call}", func(w http.ResponseWriter, r *http.Request) {
		call := chi.URLParam(r, "call")
		if call != "" {
			result := make([]map[string]string, 0)

			err := func() error {
				sqlfile := fmt.Sprintf("api/%s.sql", call)
				query, err := ioutil.ReadFile(sqlfile)
				if err != nil {
					return err
				}

				params := make(map[string]interface{})
				urlQuery := r.URL.Query()
				for name, param := range urlQuery {
					params[name] = param[0]
				}

				stmt, err := db.PrepareNamed(string(query))
				if err != nil {
					return err
				}
				rows, err := stmt.Queryx(params)
				if err != nil {
					return err
				}
				for rows.Next() {
					row := make(map[string]interface{})
					rowStrings := make(map[string]string)
					err = rows.MapScan(row)
					if err != nil {
						return err
					}
					for name, val := range row {
						switch tval := val.(type) {
						case []uint8:
							ba := make([]byte, len(tval))
							for i, v := range tval {
								ba[i] = byte(v)
							}
							rowStrings[name] = string(ba)
						default:
							return fmt.Errorf("Unknown column type %s %#v", name, spew.Sdump(val))
						}
					}
					result = append(result, rowStrings)
				}
				return nil
			}()
			resputil.JSON(w, err, result)
			return
		}
		resputil.JSON(w, "Unknown API call")
	})

	http.Serve(listener, r)
}
