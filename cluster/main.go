/*
* CursusDB
* Node
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
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
	"io"
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
	"unicode/utf8"
)

// Cursus is the CursusDB Cluster struct
type Cursus struct {
	TCPAddr         *net.TCPAddr       // TCPAddr represents the address of the clusters TCP end point
	TCPListener     *net.TCPListener   // TCPListener is the cluster TCP network listener.
	Wg              *sync.WaitGroup    // Cluster WaitGroup waits for all goroutines to finish up
	NodeConnections []*NodeConnection  // Configured and forever connected node connections until shutdown.
	SignalChannel   chan os.Signal     // Catch operating system signal
	Config          Config             // Cluster config
	TLSConfig       *tls.Config        // Cluster TLS config if TLS is true
	ContextCancel   context.CancelFunc // For gracefully shutting down
	ConfigMu        *sync.RWMutex      // Cluster config mutex
	Context         context.Context    // Main looped go routine context.  This is for listeners, event loops and so forth
}

// NodeConnection is the cluster connected to a node as a client.
type NodeConnection struct {
	Conn       *net.TCPConn    // Net connection
	SecureConn *tls.Conn       // Secure connection with TLS
	Text       *textproto.Conn // For writing and reading
	Mu         *sync.Mutex
}

// Connection is the main TCP connection struct for cluster
type Connection struct {
	Text *textproto.Conn        // Text is used for reading and writing
	Conn net.Conn               // net.Conn is a generic stream-oriented network connection.
	User map[string]interface{} // Authenticated user
}

// Config is the CursusDB cluster config struct
type Config struct {
	Nodes          []string `yaml:"nodes"`                    // Node host/ips
	Host           string   `yaml:"host"`                     // Cluster host
	TLSNode        bool     `default:"false" yaml:"tls-node"` // Connects to nodes with tls.  Nodes MUST be using tls in-order to set this to true.
	TLSCert        string   `yaml:"tls-cert"`                 // Location to TLS cert
	TLSKey         string   `yaml:"tls-key"`                  // Location to TLS key
	TLS            bool     `default:"false" yaml:"tls"`      // TLS on or off ?
	Port           int      `yaml:"port"`                     // Cluster port
	Key            string   `yaml:"key"`                      // Shared key - this key is used to encrypt data on all nodes and to authenticate with a node.
	Users          []string `yaml:"users"`                    // Array of encoded users
	NodeReaderSize int      `yaml:"node-reader-size"`         // How large of a response buffer can the cluster handle
}

// ValidatePermission validates cluster permissions aka R or RW
func (cursus *Cursus) ValidatePermission(perm string) bool {
	switch perm {
	case "R":
		return true
	case "RW":
		return true
	default:
		return false
	}
}

// NewUser creates new database user
func (cursus *Cursus) NewUser(username, password, permission string) (string, map[string]interface{}, error) {
	user := make(map[string]interface{}) // Create map with username, password, and permission
	user["username"] = username
	user["password"] = password
	var encodeUsername string

	encodeUsername = base64.StdEncoding.EncodeToString([]byte(username))

	for _, u := range cursus.Config.Users {
		if strings.Split(u, ":")[0] == encodeUsername {
			return "", user, errors.New(fmt.Sprintf("%d Database user already exists.", 103))
		}
	}

	permission = strings.TrimSpace(permission) // trim any space

	if cursus.ValidatePermission(permission) {
		user["permission"] = permission
		b, err := json.Marshal(user)
		if err != nil {
			return "", user, errors.New(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
		}

		h := sha256.New()
		h.Write(b)
		hashedUser := h.Sum(nil)

		cursus.ConfigMu.Lock()
		cursus.Config.Users = append(cursus.Config.Users, fmt.Sprintf("%s:%s", encodeUsername, base64.StdEncoding.EncodeToString(hashedUser))) // base64-encoded-username:struct-encoded-hashed
		cursus.ConfigMu.Unlock()

		return base64.StdEncoding.EncodeToString(hashedUser), user, nil
	} else {
		return "", user, errors.New(fmt.Sprintf("%d Invalid permission.", 101))
	}
}

// StartTCPListener starts the cluster's TCP/TLS listener based on configurations
func (cursus *Cursus) StartTCPListener() {
	var err error          // Local to go routine error variable
	defer cursus.Wg.Done() // Defer go routine completion

	// Resolve the string address to a TCP address
	cursus.TCPAddr, err = net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", cursus.Config.Host, cursus.Config.Port)) // Setting configured host and port
	if err != nil {
		fmt.Println("StartTCPListener()", err.Error())
		cursus.SignalChannel <- os.Interrupt // Send signal
		return
	}

	if cursus.Config.TLS {

		// Check if TLS cert and key is provided within config
		if cursus.Config.TLSCert == "" || cursus.Config.TLSKey == "" {
			fmt.Println("TCP_TLSListener():", "TLS cert and key missing.") // Log an error
			cursus.SignalChannel <- os.Interrupt                           // Send interrupt to signal channel
			return
		}

		// Load cert
		cer, err := tls.LoadX509KeyPair(cursus.Config.TLSCert, cursus.Config.TLSKey)
		if err != nil {
			fmt.Println("TCP_TLSListener():", err.Error()) // Log an error
			cursus.SignalChannel <- os.Interrupt           // Send interrupt to signal channel
			return                                         // close up go routine
		}

		cursus.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cer}}

	}

	// Start listening for TCP connections on the given address
	cursus.TCPListener, err = net.ListenTCP("tcp", cursus.TCPAddr)
	if err != nil {
		fmt.Println("StartTCPListener()", err.Error())
		cursus.SignalChannel <- os.Interrupt // Send signal
		return
	}

	for {
		if cursus.Context.Err() != nil {
			fmt.Println("ending StartTCPListener")
			cursus.TCPListener.Close()
			return
		}

		cursus.TCPListener.SetDeadline(time.Now().Add(time.Nanosecond * 100000))
		conn, err := cursus.TCPListener.Accept()
		if errors.Is(err, os.ErrDeadlineExceeded) {
			continue
		}

		// If TLS is set to true within config let's make the connection secure
		if cursus.Config.TLS {
			conn = tls.Server(conn, cursus.TLSConfig)
		}

		//Expect Authentication: username\0password\n b64 encoded
		auth, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}

		// Split AT -> Authentication:
		authSpl := strings.Split(strings.TrimSpace(auth), "Authentication:")
		if len(authSpl) != 2 { // length not equal 2?  not good return error
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 1, "Missing authentication header.")))
			conn.Close()
			continue
		}

		// Get auth value and decode.
		authValues, err := base64.StdEncoding.DecodeString(strings.TrimSpace(authSpl[1]))
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 2, "Invalid authentication value.")))
			conn.Close()
			continue
		}

		// Split AT \0 and get username and password
		authValuesSpl := strings.Split(string(authValues), "\\0")
		if len(authValuesSpl) != 2 {
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 2, "Invalid authentication value.")))
			conn.Close()
			continue
		}

		// Authenticate user
		_, u, err := cursus.AuthenticateUser(authValuesSpl[0], authValuesSpl[1])
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 4, err.Error()))) // no user match
			conn.Close()
			continue
		}

		// Write back to client that authentication was a success
		conn.Write([]byte(fmt.Sprintf("%d %s\r\n", 0, "Authentication successful.")))

		// Add to cluster waitgroup
		cursus.Wg.Add(1)
		go cursus.HandleConnection(conn, u) // Handle connection in go routine

	}
}

// AuthenticateUser checks if a user exists and returns the user
func (cursus *Cursus) AuthenticateUser(username string, password string) (string, map[string]interface{}, error) {
	// Here we are hashing the provided username and password and seeing if a user exists with either permission

	userR := make(map[string]interface{}) // Create map with username, password, and permission
	userR["username"] = username
	userR["password"] = password
	userR["permission"] = "R"

	bR, err := json.Marshal(userR)
	if err != nil {
		panic(err)
	}
	hR := sha256.New()
	hR.Write(bR)
	hashedR := hR.Sum(nil)

	userRW := make(map[string]interface{}) // Create map with username, password, and permission
	userRW["username"] = username
	userRW["password"] = password
	userRW["permission"] = "RW"

	bRW, err := json.Marshal(userRW)
	if err != nil {
		return "", userRW, errors.New(fmt.Sprintf("%d Unknown error %s", err.Error()))
	}
	hRW := sha256.New()
	hRW.Write(bRW)
	hashedRW := hRW.Sum(nil)

	for _, u := range cursus.Config.Users {
		if u == fmt.Sprintf("%s:%s", base64.StdEncoding.EncodeToString([]byte(username)), base64.StdEncoding.EncodeToString(hashedR)) {
			return u, userR, nil
		} else if u == fmt.Sprintf("%s:%s", base64.StdEncoding.EncodeToString([]byte(username)), base64.StdEncoding.EncodeToString(hashedRW)) {
			return u, userRW, nil
		}

	}

	return "", nil, errors.New("No user exists")
}

// HandleConnection handles a client connection after authentication.  A client provides queries and this method will read up to a semi-colon then forming JSON the node will understand and sending it off to every node.
func (cursus *Cursus) HandleConnection(conn net.Conn, user map[string]interface{}) {
	log.Println("new conn")
	defer cursus.Wg.Done()
	defer conn.Close()

	text := textproto.NewConn(conn)
	defer text.Close()

	query := ""

	for {
		if cursus.Context.Err() != nil {
			return
		}

		err := conn.SetReadDeadline(time.Now().Add(time.Second * 1))
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			} else {
				return
			}
		}
		read, err := text.ReadLine()
		if err != nil {
			return
		}
		if strings.HasSuffix(strings.TrimSpace(string(read)), ";") {
			query += strings.TrimSpace(string(read))
		} else {
			query += strings.TrimSpace(string(read)) + " "
		}

		if strings.HasPrefix(query, "quit") {
			return
		} else if strings.HasSuffix(query, ";") {
			fmt.Println("QUERY:", query) // Log

			switch user["permission"] {
			case "R":
			case strings.HasPrefix(query, "update"):
				text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
				return
			case strings.HasPrefix(query, "insert"):
				text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
				return
			case strings.HasPrefix(query, "delete"):
				text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
				return
			case strings.HasPrefix(query, "select"):
				goto allowed
				return
			case "RW":
				goto allowed
			}

		allowed:

			switch {
			// Query starts with insert
			case strings.HasPrefix(query, "insert "):

				// query is not valid
				// must have a full prefix of 'insert into '
				if !strings.HasPrefix(query, "insert into ") {
					text.PrintfLine(fmt.Sprintf("%d Invalid query", 3000))
					query = "" // Clear query variable and listen for another
					continue
				}

				// Regex for insert i.e coll({}) in-between parenthesis
				var insertJsonRegex = regexp.MustCompile(`\((.*?)\)`)

				insertJson := insertJsonRegex.FindStringSubmatch(query) // Get insert JSON

				collection := strings.ReplaceAll(strings.Split(query, "({\"")[0], "insert into ", "")

				if len(insertJson) != 2 {
					text.PrintfLine(fmt.Sprintf("%d Invalid query", 3000))
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
								text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
								query = ""
								continue
							}

							for _, a := range arr {
								body["values"] = append(body["values"].([]interface{}), a)
								body["keys"] = append(body["keys"].([]interface{}), strings.TrimSpace(strings.TrimSuffix(indx[1], "!"))) // add key for query
								body["oprs"] = append(body["oprs"].([]interface{}), "==")

							}

							res := cursus.QueryNodesRet(body)
							for _, r := range res {
								if !strings.EqualFold(r, "null") {
									result := make(map[string]interface{})
									result["statusCode"] = 4004
									result["message"] = fmt.Sprintf("Document already exists")

									r, _ := json.Marshal(result)
									text.PrintfLine(string(r))
									query = ""
									goto cont
								}
							}

						} else {

							body["values"] = append(body["values"].([]interface{}), kValue.FindStringSubmatch(query)[1])

							if strings.EqualFold(body["values"].([]interface{})[0].(string), "null") {
								body["values"].([]interface{})[0] = nil
							} else if cursus.IsString(body["values"].([]interface{})[0].(string)) {

								body["values"].([]interface{})[0] = strings.TrimSuffix(body["values"].([]interface{})[0].(string), "\"")
								body["values"].([]interface{})[0] = strings.TrimPrefix(body["values"].([]interface{})[0].(string), "\"")
								body["values"].([]interface{})[0] = strings.TrimSuffix(body["values"].([]interface{})[0].(string), "'")
								body["values"].([]interface{})[0] = strings.TrimPrefix(body["values"].([]interface{})[0].(string), "'")
							} else if cursus.IsBool(body["values"].([]interface{})[0].(string)) {

								b, err := strconv.ParseBool(body["values"].([]interface{})[0].(string))
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
									query = ""
									continue
								}

								body["values"].([]interface{})[0] = b
							} else if cursus.IsFloat(body["values"].([]interface{})[0].(string)) {

								f, err := strconv.ParseFloat(body["values"].([]interface{})[0].(string), 64)
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
									query = ""
									continue
								}

								body["values"].([]interface{})[0] = f
							} else if cursus.IsInt(body["values"].([]interface{})[0].(string)) {
								i, err := strconv.Atoi(body["values"].([]interface{})[0].(string))
								if err != nil {
									text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
									query = ""
									continue
								}

								body["values"].([]interface{})[0] = i

							}

							res := cursus.QueryNodesRet(body)

							for _, r := range res {
								if !strings.EqualFold(r, "null") {
									result := make(map[string]interface{})
									result["statusCode"] = 4004
									result["message"] = fmt.Sprintf("Document already exists")

									r, _ := json.Marshal(result)
									text.PrintfLine(string(r))
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

				res := cursus.QueryNodesRet(body)
				for _, r := range res {
					if !strings.EqualFold(r, "null") {
						goto retry // $id already exists
					}
				}

				goto insert
			retry:
				body["values"].([]interface{})[0] = uuid.New().String()

				res = cursus.QueryNodesRet(body)
				for _, r := range res {
					if !strings.EqualFold(r, "null") {
						goto retry // $id already exists
					}
				}

				goto insert

			insert:
				cursus.InsertIntoNode(&Connection{Conn: conn, Text: text, User: user}, strings.ReplaceAll(insertJson[1], "!\":", "\":"), collection, body["values"].([]interface{})[0].(string))

				query = ""
				continue
			case strings.HasPrefix(query, "select "):

				if !strings.Contains(query, "from ") {
					text.PrintfLine(fmt.Sprintf("%d From is required", 4006))
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

					err := cursus.QueryNodes(&Connection{Conn: conn, Text: text, User: user}, body)
					if err != nil {
						text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
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
							text.PrintfLine(fmt.Sprintf("%d Invalid query operator.", 4007))
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
						} else if cursus.IsString(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
						} else if cursus.IsBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							b, err := strconv.ParseBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = b
						} else if cursus.IsFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							f, err := strconv.ParseFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), 64)
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = f
						} else if cursus.IsInt(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {
							i, err := strconv.Atoi(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = i

						}

					}

					err := cursus.QueryNodes(&Connection{Conn: conn, Text: text, User: user}, body)
					if err != nil {
						text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
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
						text.PrintfLine(fmt.Sprintf("%d Set is missing =", 4008))
						query = ""
						goto cont4
					}

					val = strings.TrimSuffix(spl[1], ";")
					if strings.EqualFold(val.(string), "null") {
						val = nil
					} else if cursus.IsString(val.(string)) {

						val = strings.TrimSuffix(val.(string), "\"")
						val = strings.TrimPrefix(val.(string), "\"")
						val = strings.TrimSuffix(val.(string), "'")
						val = strings.TrimPrefix(val.(string), "'")
					} else if cursus.IsBool(val.(string)) {

						b, err := strconv.ParseBool(val.(string))
						if err != nil {
							text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
							query = ""
							continue
						}

						val = b
					} else if cursus.IsFloat(val.(string)) {

						f, err := strconv.ParseFloat(val.(string), 64)
						if err != nil {
							text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
							query = ""
							continue
						}

						val = f
					} else if cursus.IsInt(val.(string)) {
						i, err := strconv.Atoi(val.(string))
						if err != nil {
							text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
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
						text.PrintfLine(fmt.Sprintf("%d Invalid query operator.", 4007))
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
					} else if cursus.IsString(val.(string)) {

						val = strings.TrimSuffix(val.(string), "\"")
						val = strings.TrimPrefix(val.(string), "\"")
						val = strings.TrimSuffix(val.(string), "'")
						val = strings.TrimPrefix(val.(string), "'")
					} else if cursus.IsBool(val.(string)) {

						b, err := strconv.ParseBool(val.(string))
						if err != nil {
							text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
							query = ""
							continue
						}

						val = b
					} else if cursus.IsFloat(val.(string)) {

						f, err := strconv.ParseFloat(val.(string), 64)
						if err != nil {
							text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
							query = ""
							continue
						}

						val = f
					} else if cursus.IsInt(val.(string)) {
						i, err := strconv.Atoi(val.(string))
						if err != nil {
							text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
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

				err := cursus.QueryNodes(&Connection{Conn: conn, Text: text, User: user}, body)
				if err != nil {
					text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
					query = ""
					continue
				}

				query = ""
				continue

			case strings.HasPrefix(query, "delete "):
				// delete 1 from users where name == 'alex' && last == 'padula';

				if !strings.Contains(query, "from ") {
					text.PrintfLine(fmt.Sprintf("%d From is required", 4006))
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

					err := cursus.QueryNodes(&Connection{Conn: conn, Text: text, User: user}, body)
					if err != nil {
						text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
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
							text.PrintfLine(fmt.Sprintf("%d Invalid query operator.", 4007))
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
						} else if cursus.IsString(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "\"")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimSuffix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = strings.TrimPrefix(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), "'")
						} else if cursus.IsBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							b, err := strconv.ParseBool(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = b
						} else if cursus.IsFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

							f, err := strconv.ParseFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), 64)
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = f
						} else if cursus.IsInt(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {
							i, err := strconv.Atoi(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
							if err != nil {
								text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
								query = ""
								continue
							}

							body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = i

						}

					}

					err := cursus.QueryNodes(&Connection{Conn: conn, Text: text, User: user}, body)
					if err != nil {
						text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
						query = ""
						continue
					}

					query = ""
					continue

				}
			case strings.HasPrefix(query, "delete user "):
				splQ := strings.Split(query, "delete user ")

				if len(splQ) != 2 {
					text.PrintfLine("%d Invalid command/query.", 4005)
					query = ""
					continue
				}

				err := cursus.RemoveUser(splQ[1])
				if err != nil {
					text.PrintfLine("%d Database user %s removed successfully.", 201, splQ[1])
					query = ""
					continue
				}

				text.PrintfLine("%d No user exists with username %s.", 102, splQ[1])
				query = ""
				continue

			case strings.HasPrefix(query, "new user "): // new user username, password, RW
				splQ := strings.Split(query, "new user ")

				// now we split at comma if value is equal to 2
				if len(splQ) != 2 {
					text.PrintfLine("%d Invalid command/query.", 4005)
					query = ""
					continue
				}

				splQComma := strings.Split(splQ[1], ",")

				if len(splQComma) != 3 {
					text.PrintfLine("%d Invalid command/query.", 4005)
					query = ""
					continue
				}

				_, _, err := cursus.NewUser(strings.TrimSpace(splQComma[0]), strings.TrimSpace(splQComma[1]), strings.TrimSpace(splQComma[2]))
				if err != nil {
					text.PrintfLine(err.Error())
					query = ""
					continue
				}

				text.PrintfLine(fmt.Sprintf("%d New database user %s created successfully.", 200, strings.TrimSpace(splQComma[0])))
				query = ""
				continue

			default:
				text.PrintfLine("%d Invalid command/query.", 4005)
				query = ""
				continue

			}
		}

	}

}

// RemoveUser removes a user by username
func (cursus *Cursus) RemoveUser(username string) error {

	for j := 0; j < 50; j++ { // retry as gorm will serialize the bytes a bit different sometimes
		encodeUsername := base64.StdEncoding.EncodeToString([]byte(username))
		for i, user := range cursus.Config.Users {
			if strings.Split(user, ":")[0] == encodeUsername {
				cursus.ConfigMu.Lock()
				cursus.Config.Users[i] = cursus.Config.Users[len(cursus.Config.Users)-1]
				cursus.Config.Users[len(cursus.Config.Users)-1] = ""
				cursus.Config.Users = cursus.Config.Users[:len(cursus.Config.Users)-1]
				cursus.ConfigMu.Unlock()
				return nil
			}
		}
	}

	return errors.New("No user found")
}

// InsertIntoNode selects one node within cluster nodes and inserts json document.
func (cursus *Cursus) InsertIntoNode(connection *Connection, insert string, collection string, id string) {

	var node *NodeConnection // Node connection which will be chosen randomly
	nodeRetries := 3         // Amount of times to retry another node if the chosen node is at peak allocation

	// Setting up document hashmap
	doc := make(map[string]interface{})

	// Unmarshal insert json into hashmap
	err := json.Unmarshal([]byte(insert), &doc)
	if err != nil {
		connection.Text.PrintfLine("%d Unmarsharable JSON insert", 4000)
		return
	}

	doc["$id"] = id // We have already verified the id to not exist

	jsonMap := make(map[string]interface{}) // Return JSON

	jsonMap["document"] = doc
	jsonMap["action"] = "insert"

	jsonMap["collection"] = collection

	jsonString, err := json.Marshal(jsonMap)
	if err != nil {
		connection.Text.PrintfLine("Cannot insert. %s", err.Error())
		return
	}

	goto query
query:
	rand.Seed(time.Now().UnixNano())

	node = cursus.NodeConnections[(0 + rand.Intn((len(cursus.NodeConnections)-1)-0+1))] // Select a random node

	node.Text.PrintfLine("%s", string(jsonString)) // Send the query over

	response, err := node.Text.ReadLine()
	if err != nil {
		connection.Text.PrintfLine("%d Unknown error %s", 500, err.Error())
		return
	}

	if strings.HasPrefix(response, "100") {
		// Node was at peak allocation.
		// Picking another node and trying again
		if nodeRetries > 0 {
			nodeRetries -= 1
			goto query
		} else {
			connection.Text.PrintfLine("%d No node was available for insert.", 104)
			return
		}
	}

	connection.Text.PrintfLine(response)

}

// QueryNode queries a specific node
func (cursus *Cursus) QueryNode(n *NodeConnection, body []byte, wg *sync.WaitGroup, mu *sync.Mutex, responses *map[string]string) {
	defer wg.Done()
	n.Mu.Lock()
	defer n.Mu.Unlock()

	n.Text.Reader.R = bufio.NewReaderSize(n.Conn, cursus.Config.NodeReaderSize)
	n.Conn.SetReadDeadline(time.Now().Add(6 * time.Second))

	n.Text.PrintfLine("%s", string(body))

	line, err := n.Text.ReadLine()
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {

			goto unavailable
		} else if errors.Is(err, io.EOF) {
			goto unavailable
		}
	}

	(*responses)[n.Conn.RemoteAddr().String()] = line
	return
unavailable:
	mu.Lock()
	defer mu.Unlock()
	(*responses)[n.Conn.RemoteAddr().String()] = fmt.Sprintf(`{"statusCode": 105, "message": "Node %s unavailable."}`, n.Conn.RemoteAddr().String())
}

// IsString is a provided string a string literal?  "hello world"  OR 'hello world'
func (cursus *Cursus) IsString(str string) bool {

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
func (cursus *Cursus) IsInt(str string) bool {
	if _, err := strconv.Atoi(str); err == nil { // Atoi because, why not?
		return true
	}

	return false
}

// IsFloat is a provided float a float64?
func (cursus *Cursus) IsFloat(str string) bool {
	if _, err := strconv.ParseFloat(str, 64); err == nil {
		return true
	}

	return false
}

// IsBool is a provided bool a bool?
func (cursus *Cursus) IsBool(str string) bool {
	if _, err := strconv.ParseBool(str); err == nil {
		return true
	}

	return false
}

// QueryNodes queries all nodes in parallel and gets responses
func (cursus *Cursus) QueryNodes(connection *Connection, body map[string]interface{}) error {
	jsonString, _ := json.Marshal(body)

	responses := make(map[string]string)

	wgPara := &sync.WaitGroup{}
	muPara := &sync.Mutex{}
	for _, n := range cursus.NodeConnections {
		wgPara.Add(1)
		go cursus.QueryNode(n, jsonString, wgPara, muPara, &responses)
	}

	wgPara.Wait()

	for key, res := range responses {
		connection.Text.PrintfLine("%s: %s", key, res)
	}

	return nil
}

// QueryNodesRet queries all nodes and combines responses
func (cursus *Cursus) QueryNodesRet(body map[string]interface{}) map[string]string {
	jsonString, _ := json.Marshal(body)

	responses := make(map[string]string)

	wgPara := &sync.WaitGroup{}
	muPara := &sync.Mutex{}
	for _, n := range cursus.NodeConnections {
		wgPara.Add(1)
		go cursus.QueryNode(n, jsonString, wgPara, muPara, &responses)
	}

	wgPara.Wait()

	return responses
}

// SignalListener listeners for system signals are does a graceful shutdown
func (cursus *Cursus) SignalListener() {
	defer cursus.Wg.Done()
	for {
		select {
		case sig := <-cursus.SignalChannel:
			log.Println("received", sig)

			// Close node connections
			for _, n := range cursus.NodeConnections {
				n.Text.Close()
				n.Conn.Close()
			}
			cursus.ContextCancel()

			return

		default:
			time.Sleep(time.Nanosecond * 1000000)
		}
	}
}

// ConnectToNodes connects to configured nodes
func (cursus *Cursus) ConnectToNodes() {
	// Is the cluster connecting to nodes via TLS?
	if cursus.Config.TLSNode {

		// Iterate over configured nodes and connect
		for _, n := range cursus.Config.Nodes {

			// Resolve TCP addr based on what's provided within n ie (0.0.0.0:p)
			tcpAddr, err := net.ResolveTCPAddr("tcp", n)
			if err != nil {
				fmt.Println("ConnectToNodes():", err.Error())
				os.Exit(1)
			}

			// Dial tcp address up
			conn, err := net.DialTCP("tcp", nil, tcpAddr)
			if err != nil {
				fmt.Println("ConnectToNodes():", err.Error())
				os.Exit(1)
			}

			// We will keep the node connection alive until shutdown
			conn.SetKeepAlive(true) // forever

			// Configure TLS
			config := tls.Config{InsecureSkipVerify: false}

			// Create TLS client connection
			secureConn := tls.Client(conn, &config)

			// Authenticate with node passing shared key wrapped in base64
			conn.Write([]byte(fmt.Sprintf("Key: %s\r\n", cursus.Config.Key)))

			// Authentication response buffer
			authBuf := make([]byte, 1024)

			// Read response back from node
			r, _ := conn.Read(authBuf[:])

			// Did response start with a 0?  This indicates successful authentication
			if strings.HasPrefix(string(authBuf[:r]), "0") {

				// Add new node connection to slice
				cursus.NodeConnections = append(cursus.NodeConnections, &NodeConnection{
					Conn:       conn,
					SecureConn: secureConn,
					Text:       textproto.NewConn(secureConn),
				})

				// Report back successful connection
				fmt.Println("ConnectToNodes(): Node connection established to", conn.RemoteAddr().String())
			} else {
				// Report back invalid key.
				fmt.Println("ConnectToNodes():", "Invalid key.")
				os.Exit(1)
			}
		}
	} else {
		for _, n := range cursus.Config.Nodes {

			// Resolve TCP addr based on what's provided within n ie (0.0.0.0:p)
			tcpAddr, err := net.ResolveTCPAddr("tcp", n)
			if err != nil {
				fmt.Println("ConnectToNodes():", err.Error())
				os.Exit(1)
			}

			// Dial tcp address up
			conn, err := net.DialTCP("tcp", nil, tcpAddr)
			if err != nil {
				fmt.Println("ConnectToNodes():", err.Error())
				os.Exit(1)
			}

			// We will keep the node connection alive until shutdown
			conn.SetKeepAlive(true) // forever

			// Authenticate with node passing shared key wrapped in base64
			conn.Write([]byte(fmt.Sprintf("Key: %s\r\n", cursus.Config.Key)))

			// Authentication response buffer
			authBuf := make([]byte, 1024)

			// Read response back from node
			r, _ := conn.Read(authBuf[:])

			// Did response start with a 0?  This indicates successful authentication
			if strings.HasPrefix(string(authBuf[:r]), "0") {

				// Add new node connection to slice
				cursus.NodeConnections = append(cursus.NodeConnections, &NodeConnection{
					Conn: conn,
					Mu:   &sync.Mutex{},
					Text: textproto.NewConn(conn),
				})

				// Report back successful connection
				fmt.Println("ConnectToNodes(): Node connection established to", conn.RemoteAddr().String())
			} else {
				// Report back invalid key
				fmt.Println("ConnectToNodes():", "Invalid key.")
				os.Exit(1)
			}

		}

	}
}

func main() {
	var cursus Cursus
	cursus.SignalChannel = make(chan os.Signal, 1)
	cursus.Wg = &sync.WaitGroup{}

	cursus.Context, cursus.ContextCancel = context.WithCancel(context.Background())

	cursus.ConfigMu = &sync.RWMutex{} // Cluster config mutex

	// We check if a .cursusconfig file exists
	if _, err := os.Stat("./.cursusconfig"); errors.Is(err, os.ErrNotExist) {
		// .cursusconfig does not exist..

		cursus.Config.Port = 7681            // Default CursusDB cluster port
		cursus.Config.NodeReaderSize = 10240 // Default node reader size of 10240 bytes.. Pretty large json response
		cursus.Config.Host = "0.0.0.0"

		// Get initial database user credentials
		fmt.Println("Before starting your CursusDB cluster you must first create a database user and cluster key.  This initial database user will have read and write permissions.  To add more users use curush (The CursusDB Shell).  The cluster key is checked against what you setup on your nodes and used for data encryption.  All your nodes should share the same key you setup on your cluster.")
		fmt.Print("username> ")
		username, err := term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		// Relay entry with asterisks
		fmt.Print(strings.Repeat("*", utf8.RuneCountInString(string(username)))) // Relay input with *
		fmt.Println("")
		fmt.Print("password> ")
		password, err := term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		// Relay entry with asterisks
		fmt.Print(strings.Repeat("*", utf8.RuneCountInString(string(password)))) // Relay input with *
		fmt.Println("")
		fmt.Print("key> ")
		key, err := term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		// Relay entry with asterisks
		fmt.Print(strings.Repeat("*", utf8.RuneCountInString(string(key)))) // Relay input with *
		fmt.Println("")

		// Hash shared key
		hashedKey := sha256.Sum256(key)
		cursus.Config.Key = base64.StdEncoding.EncodeToString(append([]byte{}, hashedKey[:]...)) // Encode hashed key

		cursus.NewUser(string(username), string(password), "RW") // Create new user with RW permissions

		fmt.Println("")

		clusterConfigFile, err := os.OpenFile("./.cursusconfig", os.O_CREATE|os.O_RDWR, 0777) // Create .cursusconfig yaml file
		if err != nil {
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		defer clusterConfigFile.Close()

		// Marshal config to yaml
		yamlData, err := yaml.Marshal(&cursus.Config)
		if err != nil {
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		clusterConfigFile.Write(yamlData) // Write to yaml config
	} else { // .cursusconfig exists

		// Read .cursus config
		clusterConfigFile, err := os.ReadFile("./.cursusconfig")
		if err != nil {
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

		// Unmarshal config into cluster.config
		err = yaml.Unmarshal(clusterConfigFile, &cursus.Config)
		if err != nil {
			fmt.Println("main():", err.Error())
			os.Exit(1)
		}

	}

	// If cluster configured cluster nodes == 0, inform user to add a node
	if len(cursus.Config.Nodes) == 0 {
		fmt.Println("You must setup nodes for the Cursus to read from in your .cursusconfig file.")
		os.Exit(0)
	}

	// If port provided as flag use it instead of whats on config file
	flag.IntVar(&cursus.Config.Port, "port", cursus.Config.Port, "port for cluster")
	flag.Parse()

	cursus.ConnectToNodes()

	signal.Notify(cursus.SignalChannel, syscall.SIGINT, syscall.SIGTERM)

	cursus.Wg.Add(1)
	go cursus.SignalListener()

	cursus.Wg.Add(1)
	go cursus.StartTCPListener()

	cursus.Wg.Wait()

	os.Exit(0)

}
