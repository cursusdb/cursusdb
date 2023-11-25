/*
* CursusDB
* Cluster
* ******************************************************************
* Copyright (C) 2023 CursusDB
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
	"log"
	"math/rand"
	"net"
	"net/textproto"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Cluster type
type Cluster struct {
	Listener        net.Listener     // TCP listener
	Wg              *sync.WaitGroup  // Go routine wait group
	SignalChannel   chan os.Signal   // OS Signal channel
	Config          Config           // Cluster config (read from yaml .cursusconfig)
	NodeConnections []NodeConnection // Configured and forever connected node connections.
	Connections     []*Connection    // Client connections
	ConnectionsMu   *sync.Mutex
	NodesMu         *sync.Mutex // Global node mutex for writing unique documents.
}

// Config is the cluster config type
type Config struct {
	Nodes   []string `yaml:"nodes"` // Node host/ips
	TLSCert string   `yaml:"tls-cert"`
	TLSKey  string   `yaml:"tls-key"`
	TLS     bool     `default:"false" yaml:"tls"`
	Port    int      `yaml:"port"`
	Users   []string `yaml:"users"` // Array of encoded users
}

// NodeConnection is the cluster connected to a node as a client.
type NodeConnection struct {
	Conn *net.TCPConn    // Net connection
	Text *textproto.Conn // For writing and reading
}

// Connection is a TCP Client connection
type Connection struct {
	Conn net.Conn               // Net connection
	Text *textproto.Conn        // For writing and reading
	User map[string]interface{} // Authenticated User
}

// TCP_TLSListener start listening to TCP or TLS
func (cluster *Cluster) TCP_TLSListener() {
	defer cluster.Wg.Done() // Defer specific wait group to close up
	var err error           // error variable

	if cluster.Config.TLS {
		if cluster.Config.TLSCert == "" && cluster.Config.TLSKey == "" {
			log.Println("TCP_TLSListener():", "TLS cert and key missing.") // Log an error
			cluster.SignalChannel <- os.Interrupt                          // Send interrupt to signal channel
			return
		}

		cer, err := tls.LoadX509KeyPair(cluster.Config.TLSCert, cluster.Config.TLSKey)
		if err != nil {
			log.Println("TCP_TLSListener():", err.Error()) // Log an error
			cluster.SignalChannel <- os.Interrupt          // Send interrupt to signal channel
			return                                         // close up go routine
		}

		config := &tls.Config{Certificates: []tls.Certificate{cer}}
		cluster.Listener, err = tls.Listen("tcp", "0.0.0.0:7222", config)
		if err != nil {
			log.Println("TCP_TLSListener():", err.Error()) // Log an error
			cluster.SignalChannel <- os.Interrupt          // Send interrupt to signal channel
			return                                         // close up go routine
		}

	} else {
		cluster.Listener, err = net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", cluster.Config.Port)) // Default Cursus cluster port is 7681
		if err != nil {
			log.Println("TCP_TLSListener():", err.Error()) // Log an error
			cluster.SignalChannel <- os.Interrupt          // Send interrupt to signal channel
			return                                         // close up go routine
		}
	}

	// Starting accepting connections
	for {
		conn, err := cluster.Listener.Accept()
		if err != nil {
			return // Closed listener 99% of the time.
		}

		cluster.Wg.Add(1) // add new wg for handle connection go routine
		go cluster.HandleConnection(&Connection{
			Conn: conn,
		})

		// .. next connection
	}

}

// IsString is a provided string a string literal?  "hello world"  OR 'hello world'
func (cluster *Cluster) IsString(str string) bool {

	switch {
	case strings.HasPrefix(str, "\"") && strings.HasSuffix(str, "\""): // has " and "
		return true
	case strings.HasPrefix(str, "'") && strings.HasSuffix(str, "'"): // has ' and '
		return true
	default:
		return false
	}
}

// IsInt is a provided int an int?
func (cluster *Cluster) IsInt(str string) bool {
	if _, err := strconv.Atoi(str); err == nil { // Atoi because, why not?
		return true
	}

	return false
}

// IsFloat is a provided float a float64?
func (cluster *Cluster) IsFloat(str string) bool {
	if _, err := strconv.ParseFloat(str, 64); err == nil {
		return true
	}

	return false
}

// IsBool is a provided bool a bool?
func (cluster *Cluster) IsBool(str string) bool {
	if _, err := strconv.ParseBool(str); err == nil {
		return true
	}

	return false
}

// HandleConnection handles client connections
func (cluster *Cluster) HandleConnection(connection *Connection) {
	defer cluster.Wg.Done() // close go routine on return
	cluster.ConnectionsMu.Lock()
	cluster.Connections = append(cluster.Connections, connection) // Add connection to connections slice.
	cluster.ConnectionsMu.Unlock()

	connection.Text = textproto.NewConn(connection.Conn) // Setup writer and reader for connection

	defer connection.Text.Close() // close writer and reader on return
	defer connection.Conn.Close() // close connection on return

	defer func(conn *Connection) { // remove connection from connections slice on return

		for i, c := range cluster.Connections {
			if c == conn {
				cluster.ConnectionsMu.Lock()
				cluster.Connections = append(cluster.Connections[:i], cluster.Connections[i+1:]...)
				cluster.ConnectionsMu.Unlock()
			}
		}

	}(connection)

	// Expect Authentication: username\0password b64 encoded

	auth, err := connection.Text.ReadLine()
	if err != nil {
		connection.Text.PrintfLine("%d %s", 3, "Unable to read authentication header.")
		return
	}
	authSpl := strings.Split(auth, "Authentication:")
	if len(authSpl) != 2 {
		connection.Text.PrintfLine("%d %s", 1, "Missing authentication header.")
		return
	}

	authValues, err := base64.StdEncoding.DecodeString(strings.TrimSpace(authSpl[1]))
	if err != nil {
		connection.Text.PrintfLine("%d %s", 2, "Invalid authentication value.")
		return
	}

	authValuesSpl := strings.Split(string(authValues), "\\0")
	if len(authValuesSpl) != 2 {
		connection.Text.PrintfLine("%d %s", 2, "Invalid authentication value.")
		return
	}

	_, _, err = cluster.AuthenticateUser(authValuesSpl[0], authValuesSpl[1])
	if err != nil {
		connection.Text.PrintfLine("%d %s", 4, err.Error()) // no user exists
		return
	}

	connection.Text.PrintfLine("%d %s", 0, "Authentication successful.")

	scanner := bufio.NewScanner(connection.Conn) // Start a new scanner
	query := ""                                  // Client query variable

	// Read until ; or a single 'quit'
	for scanner.Scan() {
		query += scanner.Text()
		query = strings.Join(strings.Fields(strings.TrimSpace(query)), " ")

		if strings.HasPrefix(scanner.Text(), "quit") {
			break
		} else if strings.HasSuffix(query, ";") {
			log.Println("QUERY:", query) // Log

			wg := &sync.WaitGroup{}
			mu := &sync.Mutex{}

			switch {
			// Query starts with insert
			case strings.HasPrefix(query, "insert "):

				// query is not valid
				// must have a full prefix of 'insert into '
				if !strings.HasPrefix(query, "insert into ") {
					connection.Text.PrintfLine("Invalid query")
					query = "" // Clear query variable and listen for another
					continue
				}

				// Regex for insert i.e coll({}) in-between parenthesis
				var insertJsonRegex = regexp.MustCompile(`\((.*?)\)`)

				insertJson := insertJsonRegex.FindStringSubmatch(query) // Get insert JSON

				collection := strings.ReplaceAll(strings.Split(query, "({\"")[0], "insert into ", "")

				if len(insertJson) != 2 {
					connection.Text.PrintfLine("Invalid query")
					query = ""
					continue
				}

				// Checking if there are any !s to process
				var indexed = regexp.MustCompile(`"([^"]+!)"`) // "email!":
				// "key!" means check all nodes if this key and value exists
				// if an array "key!": [arr]
				// Cursus will check all values within the basic array.

				indexedRes := indexed.FindAllStringSubmatch(query, -1)
				// loop over unique key value pairs checking nodes
				// Returns error 4004 to client if a document exists
				for _, indx := range indexedRes {

					// Read json key VALUE(s)!
					kValue := regexp.MustCompile(fmt.Sprintf(`"%s"\s*:\s*(true|false|null|[A-Za-z]|\[.*?\]|[0-9]*[.]?[0-9]+|".*?"|'.*?')`, indx[1]))

					// body map for node submission
					body := make(map[string]interface{})
					body["action"] = "select"       // We will select 1 from all nodes with provided key value
					body["limit"] = "1"             // limit of 1 of course
					body["collection"] = collection // collection is provided collection
					body["conditions"] = []string{""}

					var interface1 []interface{} // In-order to have an interface slice in go you must set them up prior to using them.
					var interface2 []interface{} // ^
					var interface3 []interface{} // ^

					body["keys"] = interface1   // We send nodes an array of keys to query
					body["oprs"] = interface2   // We send nodes an array of oprs to use for query
					body["values"] = interface3 // Values for query
					// There must be equal keys, oprs, and values.

					body["keys"] = append(body["keys"].([]interface{}), strings.TrimSpace(strings.TrimSuffix(indx[1], "!"))) // add key for query
					body["oprs"] = append(body["oprs"].([]interface{}), "==")                                                // == obviously

					body["lock"] = true // lock on read.  There can be many clusters reading at one time.  This helps setup uniqueness across all nodes if indexes are required

					if len(kValue.FindStringSubmatch(query)) > 0 {
						if strings.HasPrefix(kValue.FindStringSubmatch(query)[1], "[") && strings.HasSuffix(kValue.FindStringSubmatch(query)[1], "]") {
							var arr []interface{}
							err := json.Unmarshal([]byte(kValue.FindStringSubmatch(query)[1]), &arr)
							if err != nil {
								connection.Text.PrintfLine("Something went wrong. %s", err.Error())
								query = ""
								continue
							}

							for _, a := range arr {
								body["values"] = append(body["values"].([]interface{}), a)
								body["keys"] = append(body["keys"].([]interface{}), strings.TrimSpace(strings.TrimSuffix(indx[1], "!"))) // add key for query
								body["oprs"] = append(body["oprs"].([]interface{}), "==")

							}

							cluster.NodesMu.Lock()
							res := cluster.QueryNodesRet(connection, body, wg, mu)
							for _, r := range res {
								if !strings.EqualFold(r, "null") {
									result := make(map[string]interface{})
									result["statusCode"] = 4004
									result["message"] = fmt.Sprintf("Document already exists")

									r, _ := json.Marshal(result)
									connection.Text.PrintfLine(string(r))
									query = ""
									goto cont
								}
							}
							cluster.NodesMu.Unlock()

						} else {

							body["values"] = append(body["values"].([]interface{}), kValue.FindStringSubmatch(query)[1])

							if strings.EqualFold(body["values"].([]interface{})[0].(string), "null") {
								body["values"].([]interface{})[0] = nil
							} else if cluster.IsString(body["values"].([]interface{})[0].(string)) {

								body["values"].([]interface{})[0] = strings.TrimSuffix(body["values"].([]interface{})[0].(string), "\"")
								body["values"].([]interface{})[0] = strings.TrimPrefix(body["values"].([]interface{})[0].(string), "\"")
								body["values"].([]interface{})[0] = strings.TrimSuffix(body["values"].([]interface{})[0].(string), "'")
								body["values"].([]interface{})[0] = strings.TrimPrefix(body["values"].([]interface{})[0].(string), "'")
							} else if cluster.IsBool(body["values"].([]interface{})[0].(string)) {

								b, err := strconv.ParseBool(body["values"].([]interface{})[0].(string))
								if err != nil {
									connection.Text.PrintfLine("Something went wrong. %s", err.Error())
									query = ""
									continue
								}

								body["values"].([]interface{})[0] = b
							} else if cluster.IsFloat(body["values"].([]interface{})[0].(string)) {

								f, err := strconv.ParseFloat(body["values"].([]interface{})[0].(string), 64)
								if err != nil {
									connection.Text.PrintfLine("Something went wrong. %s", err.Error())
									query = ""
									continue
								}

								body["values"].([]interface{})[0] = f
							} else if cluster.IsInt(body["values"].([]interface{})[0].(string)) {
								i, err := strconv.Atoi(body["values"].([]interface{})[0].(string))
								if err != nil {
									connection.Text.PrintfLine("Something went wrong. %s", err.Error())
									query = ""
									continue
								}

								body["values"].([]interface{})[0] = i

							}
							cluster.NodesMu.Lock()
							res := cluster.QueryNodesRet(connection, body, wg, mu)
							cluster.NodesMu.Unlock()
							for _, r := range res {
								if !strings.EqualFold(r, "null") {
									result := make(map[string]interface{})
									result["statusCode"] = 4004
									result["message"] = fmt.Sprintf("Document already exists")

									r, _ := json.Marshal(result)
									connection.Text.PrintfLine(string(r))
									query = ""
									goto cont
								}
							}
						}
					}
				}

				goto ok

			cont:
				continue

			ok:
				body := make(map[string]interface{})

				var interface1 []interface{}
				var interface2 []interface{}
				var interface3 []interface{}
				body["action"] = "select"
				body["limit"] = "1"
				body["collection"] = collection
				body["conditions"] = []string{""}

				body["keys"] = interface1
				body["keys"] = append(body["keys"].([]interface{}), "$id")
				body["oprs"] = interface2
				body["oprs"] = append(body["oprs"].([]interface{}), "==")

				body["lock"] = true // lock on read.  There can be many clusters reading at one time.  This helps setup uniqueness across all nodes
				body["values"] = interface3
				body["values"] = append(body["values"].([]interface{}), uuid.New().String())

				cluster.NodesMu.Lock()
				res := cluster.QueryNodesRet(connection, body, wg, mu)
				for _, r := range res {
					if !strings.EqualFold(r, "null") {
						cluster.NodesMu.Unlock()
						goto retry // $id already exists
					}
				}
				cluster.NodesMu.Unlock()

				goto insert
			retry:
				body["values"].([]interface{})[0] = uuid.New().String()
				cluster.NodesMu.Lock()
				res = cluster.QueryNodesRet(connection, body, wg, mu)
				for _, r := range res {
					if !strings.EqualFold(r, "null") {
						cluster.NodesMu.Unlock()
						goto retry // $id already exists
					}
				}

				cluster.NodesMu.Unlock()
				goto insert

			insert:
				cluster.InsertIntoNode(connection, strings.ReplaceAll(insertJson[1], "!\":", "\":"), collection, body["values"].([]interface{})[0].(string))

				query = ""
				continue
			case strings.HasPrefix(query, "select "):

				if !strings.Contains(query, "from ") {
					connection.Text.PrintfLine("from is required!")
					query = ""
					continue
				}

				querySplit := strings.Split(strings.ReplaceAll(strings.Join(strings.Fields(strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(query, "where", ""), "from", ""))), " "), "from", ""), " ")

				if !strings.Contains(query, "where ") {
					body := make(map[string]interface{})
					body["action"] = querySplit[0]
					body["limit"] = querySplit[1]
					body["collection"] = strings.TrimSuffix(querySplit[2], ";")
					var interface1 []interface{}
					var interface2 []interface{}
					var interface3 []interface{}

					body["keys"] = interface1
					body["oprs"] = interface2
					body["values"] = interface3
					body["conditions"] = []string{""}
					body["lock"] = false // lock on read.  There can be many clusters reading at one time.

					err := cluster.QueryNodes(connection, body, wg, mu)
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}

					query = ""
					continue
				} else {
					r, _ := regexp.Compile("[\\&&\\||]+")
					andOrSplit := r.Split(query, -1)

					body := make(map[string]interface{})
					body["action"] = querySplit[0]
					body["limit"] = querySplit[1]
					body["collection"] = querySplit[2]
					body["conditions"] = []string{"*"}

					var interface1 []interface{}
					var interface2 []interface{}
					var interface3 []interface{}

					body["keys"] = interface1
					body["oprs"] = interface2
					body["values"] = interface3

					for k, s := range andOrSplit {
						querySplitNested := strings.Split(strings.ReplaceAll(strings.Join(strings.Fields(strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(s, "where", ""), "from", ""))), " "), "from", ""), " ")

						body["keys"] = append(body["keys"].([]interface{}), querySplitNested[len(querySplitNested)-3])
						body["oprs"] = append(body["oprs"].([]interface{}), querySplitNested[len(querySplitNested)-2])
						body["lock"] = false // lock on read.  There can be many clusters reading at one time.

						switch {
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "=="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "!="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">"):
						default:
							connection.Text.PrintfLine("Invalid query operator.")
							query = ""
							goto cont2
						}

						goto skip

					cont2:
						continue

					skip:

						body["values"] = append(body["values"].([]interface{}), strings.TrimSuffix(querySplitNested[len(querySplitNested)-1], ";"))

						if k < len(andOrSplit)-1 {
							lindx := strings.LastIndex(query, fmt.Sprintf("%v", body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))
							valLen := len(fmt.Sprintf("%v", body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))

							body["conditions"] = append(body["conditions"].([]string), strings.TrimSpace(query[lindx+valLen:lindx+valLen+3]))
						}

						if strings.EqualFold(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "null") {
							body["values"].([]interface{})[k] = nil
						} else if cluster.IsString(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
						} else if cluster.IsBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							b, err := strconv.ParseBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								connection.Text.PrintfLine("Something went wrong. %s", err.Error())
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = b
						} else if cluster.IsFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							f, err := strconv.ParseFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), 64)
							if err != nil {
								connection.Text.PrintfLine("Something went wrong. %s", err.Error())
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = f
						} else if cluster.IsInt(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {
							i, err := strconv.Atoi(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								connection.Text.PrintfLine("Something went wrong. %s", err.Error())
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = i

						}

					}

					err := cluster.QueryNodes(connection, body, wg, mu)
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}

					query = ""
					continue

				}

			case strings.HasPrefix(query, "update "):
				// update 1 in users where name == 'jackson' set name = 'alex';
				querySplit := strings.Split(strings.ReplaceAll(strings.Join(strings.Fields(strings.TrimSpace(strings.ReplaceAll(query, "in", ""))), " "), "from", ""), " ")

				// update 1 in users where name == 'jackson' && age == 44 set name = 'alex', age = 28;
				var setStartIndex uint
				for seti, t := range querySplit {
					if t == "set" {
						setStartIndex = uint(seti)
					}
				}

				body := make(map[string]interface{})
				body["action"] = querySplit[0]
				body["limit"] = querySplit[1]
				body["collection"] = querySplit[2]
				body["conditions"] = []string{}

				var interface1 []interface{}
				var interface2 []interface{}
				var interface3 []interface{}
				var interface4 []interface{}
				var interface5 []interface{}
				body["keys"] = interface1
				body["oprs"] = interface2
				body["values"] = interface3
				body["update-keys"] = interface4
				body["new-values"] = interface5

				conditions := querySplit[4:setStartIndex]
				newValues := strings.Split(strings.ReplaceAll(strings.Join(querySplit[setStartIndex:], " "), "set ", ""), ",")

				for _, nvSet := range newValues {
					spl := strings.Split(nvSet, " = ")
					body["update-keys"] = append(body["update-keys"].([]interface{}), strings.TrimSpace(spl[0]))
					var val interface{}
					if len(spl) != 2 {
						connection.Text.PrintfLine("Set is missing =")
						query = ""
						goto cont4
					}

					val = strings.TrimSuffix(spl[1], ";")
					if strings.EqualFold(val.(string), "null") {
						val = nil
					} else if cluster.IsString(val.(string)) {

						val = strings.TrimSuffix(val.(string), "\"")
						val = strings.TrimPrefix(val.(string), "\"")
						val = strings.TrimSuffix(val.(string), "'")
						val = strings.TrimPrefix(val.(string), "'")
					} else if cluster.IsBool(val.(string)) {

						b, err := strconv.ParseBool(val.(string))
						if err != nil {
							connection.Text.PrintfLine("Something went wrong. %s", err.Error())
							query = ""
							continue
						}

						val = b
					} else if cluster.IsFloat(val.(string)) {

						f, err := strconv.ParseFloat(val.(string), 64)
						if err != nil {
							connection.Text.PrintfLine("Something went wrong. %s", err.Error())
							query = ""
							continue
						}

						val = f
					} else if cluster.IsInt(val.(string)) {
						i, err := strconv.Atoi(val.(string))
						if err != nil {
							connection.Text.PrintfLine("Something went wrong. %s", err.Error())
							query = ""
							continue
						}

						val = i

					}
					body["new-values"] = append(body["new-values"].([]interface{}), val)
				}

				goto skip3

			cont4:
				continue

			skip3:

				r, _ := regexp.Compile("[\\&&\\||]+")
				andOrSplit := r.Split(strings.Join(conditions, " "), -1)

				for k, s := range andOrSplit {
					querySplitNested := strings.Split(strings.ReplaceAll(strings.Join(strings.Fields(strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(s, "where", ""), "from", ""))), " "), "from", ""), " ")

					body["keys"] = append(body["keys"].([]interface{}), querySplitNested[len(querySplitNested)-3])
					body["oprs"] = append(body["oprs"].([]interface{}), querySplitNested[len(querySplitNested)-2])
					body["lock"] = false // lock on read.  There can be many clusters reading at one time.

					switch {
					case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "=="):
					case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "!="):
					case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<="):
					case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">="):
					case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<"):
					case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">"):
					default:
						connection.Text.PrintfLine("Invalid query operator.")
						query = ""
						goto cont3
					}

					goto skip2

				cont3:
					continue

				skip2:
					var val interface{}
					val = strings.TrimSuffix(strings.TrimSuffix(querySplitNested[len(querySplitNested)-1], ";"), ";")
					if strings.EqualFold(val.(string), "null") {
						val = nil
					} else if cluster.IsString(val.(string)) {

						val = strings.TrimSuffix(val.(string), "\"")
						val = strings.TrimPrefix(val.(string), "\"")
						val = strings.TrimSuffix(val.(string), "'")
						val = strings.TrimPrefix(val.(string), "'")
					} else if cluster.IsBool(val.(string)) {

						b, err := strconv.ParseBool(val.(string))
						if err != nil {
							connection.Text.PrintfLine("Something went wrong. %s", err.Error())
							query = ""
							continue
						}

						val = b
					} else if cluster.IsFloat(val.(string)) {

						f, err := strconv.ParseFloat(val.(string), 64)
						if err != nil {
							connection.Text.PrintfLine("Something went wrong. %s", err.Error())
							query = ""
							continue
						}

						val = f
					} else if cluster.IsInt(val.(string)) {
						i, err := strconv.Atoi(val.(string))
						if err != nil {
							connection.Text.PrintfLine("Something went wrong. %s", err.Error())
							query = ""
							continue
						}

						val = i

					}
					body["values"] = append(body["values"].([]interface{}), val)

					if k < len(andOrSplit)-1 {
						lindx := strings.LastIndex(query, fmt.Sprintf("%v", body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))
						valLen := len(fmt.Sprintf("%v", body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))

						body["conditions"] = append(body["conditions"].([]string), strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(query[lindx+valLen:lindx+valLen+4]), "'", ""), "\"", "")))
					}
				}

				err := cluster.QueryNodes(connection, body, wg, mu)
				if err != nil {
					connection.Text.PrintfLine("Something went wrong. %s", err.Error())
					query = ""
					continue
				}

				query = ""
				continue

			case strings.HasPrefix(query, "delete "):
				// delete 1 from users where name == 'alex' && last == 'padula';

				if !strings.Contains(query, "from ") {
					connection.Text.PrintfLine("from is required!")
					query = ""
					continue
				}

				querySplit := strings.Split(strings.ReplaceAll(strings.Join(strings.Fields(strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(query, "where", ""), "from", ""))), " "), "from", ""), " ")

				if !strings.Contains(query, "where ") {
					body := make(map[string]interface{})
					body["action"] = querySplit[0]
					body["limit"] = querySplit[1]
					body["collection"] = strings.TrimSuffix(querySplit[2], ";")
					var interface1 []interface{}
					var interface2 []interface{}
					var interface3 []interface{}

					body["keys"] = interface1
					body["oprs"] = interface2
					body["values"] = interface3
					body["conditions"] = []string{""}
					body["lock"] = false

					err := cluster.QueryNodes(connection, body, wg, mu)
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}

					query = ""
					continue
				} else {
					r, _ := regexp.Compile("[\\&&\\||]+")
					andOrSplit := r.Split(query, -1)

					body := make(map[string]interface{})
					body["action"] = querySplit[0]
					body["limit"] = querySplit[1]
					body["collection"] = querySplit[2]
					body["conditions"] = []string{"*"}

					var interface1 []interface{}
					var interface2 []interface{}
					var interface3 []interface{}

					body["keys"] = interface1
					body["oprs"] = interface2
					body["values"] = interface3

					for k, s := range andOrSplit {
						querySplitNested := strings.Split(strings.ReplaceAll(strings.Join(strings.Fields(strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(s, "where", ""), "from", ""))), " "), "from", ""), " ")

						body["keys"] = append(body["keys"].([]interface{}), querySplitNested[len(querySplitNested)-3])
						body["oprs"] = append(body["oprs"].([]interface{}), querySplitNested[len(querySplitNested)-2])
						body["lock"] = false // lock on read.  There can be many clusters reading at one time.

						switch {
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "=="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "!="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">="):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), "<"):
						case strings.EqualFold(body["oprs"].([]interface{})[k].(string), ">"):
						default:
							connection.Text.PrintfLine("Invalid query operator.")
							query = ""
							goto cont5
						}

						goto skip4

					cont5:
						continue

					skip4:

						body["values"] = append(body["values"].([]interface{}), strings.TrimSuffix(querySplitNested[len(querySplitNested)-1], ";"))

						if k < len(andOrSplit)-1 {
							lindx := strings.LastIndex(query, fmt.Sprintf("%v", body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))
							valLen := len(fmt.Sprintf("%v", body["values"].([]interface{})[len(body["values"].([]interface{}))-1]))

							body["conditions"] = append(body["conditions"].([]string), strings.TrimSpace(query[lindx+valLen:lindx+valLen+3]))
						}

						if strings.EqualFold(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "null") {
							body["values"].([]interface{})[k] = nil
						} else if cluster.IsString(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
						} else if cluster.IsBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							b, err := strconv.ParseBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								connection.Text.PrintfLine("Something went wrong. %s", err.Error())
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = b
						} else if cluster.IsFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							f, err := strconv.ParseFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), 64)
							if err != nil {
								connection.Text.PrintfLine("Something went wrong. %s", err.Error())
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = f
						} else if cluster.IsInt(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {
							i, err := strconv.Atoi(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								connection.Text.PrintfLine("Something went wrong. %s", err.Error())
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = i

						}

					}

					err := cluster.QueryNodes(connection, body, wg, mu)
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}

					query = ""
					continue

				}

			}
		}

	}

}

func (cluster *Cluster) QueryNodes(connection *Connection, body map[string]interface{}, wg *sync.WaitGroup, mu *sync.Mutex) error {
	jsonString, _ := json.Marshal(body)

	responses := make(map[string]string)

	for _, n := range cluster.NodeConnections {
		wg.Add(1)
		go cluster.QueryNode(wg, n, jsonString, responses, mu)
	}

	wg.Wait()

	for key, res := range responses {
		connection.Text.PrintfLine("%s: %s", key, res)
	}

	return nil
}

func (cluster *Cluster) QueryNodesRet(connection *Connection, body map[string]interface{}, wg *sync.WaitGroup, mu *sync.Mutex) map[string]string {
	jsonString, _ := json.Marshal(body)

	responses := make(map[string]string)

	for _, n := range cluster.NodeConnections {
		wg.Add(1)
		go cluster.QueryNode(wg, n, jsonString, responses, mu)
	}

	wg.Wait()

	return responses
}

// InsertIntoNode selects one node within cluster nodes and inserts json document.
func (cluster *Cluster) InsertIntoNode(connection *Connection, insert string, collection string, id string) {
	doc := make(map[string]interface{})
	err := json.Unmarshal([]byte(insert), &doc)
	if err != nil {
		connection.Text.PrintfLine("Cannot insert. %s", err.Error())
		return
	}

	doc["$id"] = id

	jsonMap := make(map[string]interface{})

	jsonMap["document"] = doc
	jsonMap["action"] = "insert"

	jsonMap["collection"] = collection

	jsonString, err := json.Marshal(jsonMap)
	if err != nil {
		connection.Text.PrintfLine("Cannot insert. %s", err.Error())
		return
	}

	rand.Seed(time.Now().UnixNano())
	node := cluster.NodeConnections[(0 + rand.Intn((len(cluster.NodeConnections)-1)-0+1))]
	node.Text.PrintfLine("%s", string(jsonString))

	response, err := node.Text.ReadLine()
	if err != nil {
		connection.Text.PrintfLine("Cannot insert. %s", err.Error())
		return
	}

	connection.Text.PrintfLine(response)

}

func (cluster *Cluster) QueryNode(wg *sync.WaitGroup, n NodeConnection, body []byte, responses map[string]string, mu *sync.Mutex) {
	defer wg.Done()

	n.Text.PrintfLine("%s", string(body))

	line, err := n.Text.ReadLine()
	if err != nil {
		return
	}

	mu.Lock()
	defer mu.Unlock()
	responses[n.Conn.RemoteAddr().String()] = line

}

func (cluster *Cluster) SignalListener() {
	defer cluster.Wg.Done()

	for {
		select {
		case sig := <-cluster.SignalChannel:
			log.Println("received", sig)
			log.Println("closing", len(cluster.Connections), "connections")
			for _, c := range cluster.Connections {
				c.Text.Close()
				c.Conn.Close()
			}

			for _, c := range cluster.NodeConnections {
				c.Text.Close()
				c.Conn.Close()
			}

			if cluster.Listener != nil {
				cluster.Listener.Close()
			}
			return
		default:
			time.Sleep(time.Millisecond * 125)
		}
	}
}

func (cluster *Cluster) ConnectToNodes() {
	for _, n := range cluster.Config.Nodes {
		tcpAddr, err := net.ResolveTCPAddr("tcp", n)
		if err != nil {
			log.Println("ConnectToNodes():", err.Error())
			cluster.SignalChannel <- os.Interrupt
			return
		}

		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			log.Println("ConnectToNodes():", err.Error())
			cluster.SignalChannel <- os.Interrupt
			return
		}

		conn.SetKeepAlive(true) // forever

		cluster.NodeConnections = append(cluster.NodeConnections, NodeConnection{
			Conn: conn,
			Text: textproto.NewConn(conn),
		})

		log.Println("Node connection established to", conn.RemoteAddr().String())

	}
}

func (cluster *Cluster) NewUser(username, password, permission string) (string, map[string]interface{}, error) {
	user := make(map[string]interface{}) // Create map with username, password, and permission
	user["username"] = username
	user["password"] = password

	if cluster.ValidatePermission(permission) {
		user["permission"] = permission
		b := bytes.Buffer{}
		e := gob.NewEncoder(&b)

		err := e.Encode(user)
		if err != nil {
			return "", user, err
		}

		cluster.Config.Users = append(cluster.Config.Users, base64.StdEncoding.EncodeToString(b.Bytes()))

		return base64.StdEncoding.EncodeToString(b.Bytes()), user, nil
	} else {
		return "", user, errors.New("invalid permission")
	}
}

func (cluster *Cluster) AuthenticateUser(username string, password string) (string, map[string]interface{}, error) {

	for i := 0; i < 5; i++ { // retry as gob will sometimes provide wrong serialization
		userR := make(map[string]interface{}) // Create map with username, password, and permission
		userR["username"] = username
		userR["password"] = password
		userR["permission"] = "R"
		bR := bytes.Buffer{}
		e := gob.NewEncoder(&bR)

		err := e.Encode(userR)
		if err != nil {
			return "", userR, err
		}

		userRW := make(map[string]interface{}) // Create map with username, password, and permission
		userRW["username"] = username
		userRW["password"] = password
		userRW["permission"] = "RW"
		bRW := bytes.Buffer{}
		e = gob.NewEncoder(&bRW)

		err = e.Encode(userRW)
		if err != nil {
			return "", userRW, err
		}

		for _, u := range cluster.Config.Users {
			if u == base64.StdEncoding.EncodeToString(bR.Bytes()) {
				return u, userR, nil
			} else if u == base64.StdEncoding.EncodeToString(bRW.Bytes()) {
				return u, userRW, nil
			}
		}
	}

	return "", nil, errors.New("No user exists")
}

func (cluster *Cluster) ValidatePermission(perm string) bool {
	switch perm {
	case "R":
		return true
	case "RW":
		return true
	default:
		return false
	}
}

func main() {
	var cluster Cluster

	if _, err := os.Stat("./.cursusconfig"); errors.Is(err, os.ErrNotExist) {

		cluster.Config.Port = 7681

		fmt.Println("Before starting your CursusDB cluster you must first create a database user.  This initial database user will have read and write permissions.  To add more users use curush (The CursusDB Shell).")
		fmt.Print("Username>")
		username, err := term.ReadPassword(syscall.Stdin)
		if err != nil {
			os.Exit(1)
		}
		fmt.Println("")
		fmt.Print("Password>")
		password, err := term.ReadPassword(syscall.Stdin)
		if err != nil {
			os.Exit(1)
		}

		cluster.NewUser(string(username), string(password), "RW")

		fmt.Println("")

		clusterConfigFile, err := os.OpenFile("./.cursusconfig", os.O_CREATE|os.O_RDWR, 0777)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		defer clusterConfigFile.Close()

		yamlData, err := yaml.Marshal(&cluster.Config)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		clusterConfigFile.Write(yamlData)
	} else {
		clusterConfigFile, err := os.ReadFile("./.cursusconfig")
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		err = yaml.Unmarshal(clusterConfigFile, &cluster.Config)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

	}

	if len(cluster.Config.Nodes) == 0 {
		fmt.Println("You must setup nodes for the Cursus to read from in your .cursusconfig file.")
		os.Exit(0)
	}

	cluster.ConnectionsMu = &sync.Mutex{}
	cluster.NodesMu = &sync.Mutex{}

	flag.IntVar(&cluster.Config.Port, "port", cluster.Config.Port, "port for cluster")
	flag.Parse()

	cluster.ConnectToNodes()

	cluster.SignalChannel = make(chan os.Signal, 1)

	signal.Notify(cluster.SignalChannel, syscall.SIGINT, syscall.SIGTERM)
	cluster.Wg = &sync.WaitGroup{}

	cluster.Wg.Add(1)
	go cluster.SignalListener()

	cluster.Wg.Add(1)
	go cluster.TCP_TLSListener()

	cluster.Wg.Wait()

}
