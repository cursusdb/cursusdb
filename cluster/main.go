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
	TCPAddr           *net.TCPAddr           // Cluster TCPAddr
	TCPListener       *net.TCPListener       // Cluster TCPListener
	Wg                *sync.WaitGroup        // Global cluster waitgroup
	SignalChannel     chan os.Signal         // Channel for OS signals
	ConnectionQueue   map[string]*Connection // Hashmap of current connections
	NodeConnections   []NodeConnection       // Configured and forever connected node connections until shutdown.
	ConnectionQueueMu *sync.RWMutex          // Connection queue mutex
	ConfigMu          *sync.RWMutex          // Cluster config mutex
	ConnectionChannel chan *Connection       // Connection channel for ConnectionEventWorker
	NodesMu           *sync.Mutex            // Global cluster nodes mutex
	Config            Config                 // Cluster config
	ContextCancel     context.CancelFunc     // For gracefully shutting down
	Context           context.Context        // Main looped go routine context.  This is for listeners, event loops and so forth
	TLSConfig         *tls.Config            // Cluster TLS config if TLS is true
}

// Config is the CursusDB cluster config struct
type Config struct {
	Nodes                  []string `yaml:"nodes"`                    // Node host/ips
	Host                   string   `yaml:"host"`                     // Cluster host
	TLSNode                bool     `default:"false" yaml:"tls-node"` // Connects to nodes with tls.  Nodes MUST be using tls in-order to set this to true.
	TLSCert                string   `yaml:"tls-cert"`                 // Location to TLS cert
	TLSKey                 string   `yaml:"tls-key"`                  // Location to TLS key
	TLS                    bool     `default:"false" yaml:"tls"`      // TLS on or off ?
	Port                   int      `yaml:"port"`                     // Cluster port
	Key                    string   `yaml:"key"`                      // Shared key - this key is used to encrypt data on all nodes and to authenticate with a node.
	Users                  []string `yaml:"users"`                    // Array of encoded users
	NodeReaderSize         int      `yaml:"node-reader-size"`         // How large of a response buffer can the cluster handle
	ConnectionQueueWorkers int      `yaml:"connection-queue-workers"` // Amount of go routines to listen to events.  The worker distributes connections iteratively
}

// Connection is a TCP connection struct
type Connection struct {
	Text *textproto.Conn // Connection writer and reader
	Conn net.Conn        // Connection
	User map[string]interface{}
}

// NodeConnection is the cluster connected to a node as a client.
type NodeConnection struct {
	Conn       *net.TCPConn    // Net connection
	SecureConn *tls.Conn       // Secure connection with TLS
	Text       *textproto.Conn // For writing and reading
}

// StartTCP_TLSListener start listening on TCP or TLS on provided port
func (cursus *Cursus) StartTCP_TLSListener() {
	var err error // Local to function error variable

	defer cursus.Wg.Done() // defer finishing up go routine

	// Resolve the string address to a TCP address
	cursus.TCPAddr, err = net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", cursus.Config.Host, cursus.Config.Port))

	if err != nil {
		fmt.Println("StartTCP_TLSListener():", err)
		cursus.SignalChannel <- os.Interrupt
		return
	}

	// Cluster is configured for TLS
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
		fmt.Println("TCP_TLSListener():", err.Error()) // Log an error
		cursus.SignalChannel <- os.Interrupt           // Send interrupt to signal channel
		return                                         // close up go routine
	}

	for {

		if cursus.Context.Err() != nil {
			cursus.TCPListener.Close()
			close(cursus.ConnectionChannel)

			for _, c := range cursus.ConnectionQueue {
				c.Conn.Close()
			}

			return
		}

		cursus.TCPListener.SetDeadline(time.Now().Add(time.Nanosecond * 1000000)) // 2000 connections a second
		conn, err := cursus.TCPListener.Accept()
		if errors.Is(err, os.ErrDeadlineExceeded) {
			continue
		}

		// If TLS is set to true within config let's make the connection secure
		if cursus.Config.TLS {
			conn = tls.Server(conn, cursus.TLSConfig)
		}

		connection := &Connection{Conn: conn, Text: textproto.NewConn(conn)}

		//Expect Authentication: username\0password b64 encoded
		auth, err := connection.Text.ReadLine()
		if err != nil {
			connection.Text.PrintfLine("%d %s", 3, "Unable to read authentication header.")
			connection.Text.Close()
			connection.Conn.Close()
			continue
		}

		authSpl := strings.Split(auth, "Authentication:")
		if len(authSpl) != 2 {
			connection.Text.PrintfLine("%d %s", 1, "Missing authentication header.")
			connection.Text.Close()
			connection.Conn.Close()
			continue
		}

		authValues, err := base64.StdEncoding.DecodeString(strings.TrimSpace(authSpl[1]))
		if err != nil {
			connection.Text.PrintfLine("%d %s", 2, "Invalid authentication value.")
			connection.Text.Close()
			connection.Conn.Close()
			continue
		}

		authValuesSpl := strings.Split(string(authValues), "\\0")
		if len(authValuesSpl) != 2 {
			connection.Text.PrintfLine("%d %s", 2, "Invalid authentication value.")
			connection.Text.Close()
			connection.Conn.Close()
			continue
		}

		_, connection.User, err = cursus.AuthenticateUser(authValuesSpl[0], authValuesSpl[1])
		if err != nil {
			connection.Text.PrintfLine("%d %s", 4, err.Error()) // no user exists
			connection.Text.Close()
			connection.Conn.Close()
			continue
		}

		connection.Text.PrintfLine("%d %s", 0, "Authentication successful.")

		// Add to connection queue
		cursus.ConnectionQueueMu.Lock()
		cursus.ConnectionQueue[conn.RemoteAddr().String()] = connection
		cursus.ConnectionQueueMu.Unlock()
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

	var node NodeConnection // Node connection which will be chosen randomly
	nodeRetries := 3        // Amount of times to retry another node if the chosen node is at peak allocation

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
func (cursus *Cursus) QueryNode(wg *sync.WaitGroup, n NodeConnection, body []byte, responses map[string]string, mu *sync.Mutex) {
	defer wg.Done()

	n.Text.Reader.R = bufio.NewReaderSize(n.Conn, cursus.Config.NodeReaderSize)

	n.Text.PrintfLine("%s", string(body))
	err := n.Conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err != nil {
		fmt.Println("QueryNode(): ", err.Error())
		return
	}

	line, err := n.Text.ReadLine()
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			goto unavailable
		} else if errors.Is(err, io.EOF) {
			goto unavailable
		}
		return
	}

	mu.Lock()
	defer mu.Unlock()
	responses[n.Conn.RemoteAddr().String()] = line
	return
unavailable:
	responses[n.Conn.RemoteAddr().String()] = fmt.Sprintf(`{"statusCode": 105, "message": "Node %s unavailable."}`, n.Conn.RemoteAddr().String())
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
func (cursus *Cursus) QueryNodes(connection *Connection, body map[string]interface{}, wg *sync.WaitGroup, mu *sync.Mutex) error {
	jsonString, _ := json.Marshal(body)

	responses := make(map[string]string)

	for _, n := range cursus.NodeConnections {
		wg.Add(1)
		go cursus.QueryNode(wg, n, jsonString, responses, mu)
	}

	wg.Wait()

	for key, res := range responses {
		connection.Text.PrintfLine("%s: %s", key, res)
	}

	return nil
}

// QueryNodesRet queries all nodes and combines responses
func (cursus *Cursus) QueryNodesRet(connection *Connection, body map[string]interface{}, wg *sync.WaitGroup, mu *sync.Mutex) map[string]string {
	jsonString, _ := json.Marshal(body)

	responses := make(map[string]string)

	for _, n := range cursus.NodeConnections {
		wg.Add(1)
		go cursus.QueryNode(wg, n, jsonString, responses, mu)
	}

	wg.Wait()

	return responses
}

// ConnectionEventWorker distributes connections to goroutines listening for connection events.
func (cursus *Cursus) ConnectionEventWorker() {
	defer cursus.Wg.Done()
	for {
		if cursus.Context.Err() != nil {
			return
		}

		if len(cursus.ConnectionQueue) > 0 {
			for _, c := range cursus.ConnectionQueue {
				cursus.ConnectionChannel <- c
				time.Sleep(time.Nanosecond * 1000000)
			}
		}
		time.Sleep(time.Nanosecond * 1000000)
	}

}

// ConnectionEventLoop listens to currently connected client events
func (cursus *Cursus) ConnectionEventLoop(i int) {
	defer cursus.Wg.Done()
	for {
		select {
		case c := <-cursus.ConnectionChannel:
			if cursus.Context.Err() != nil {
				return
			}

			if c != nil {
				err := c.Conn.SetReadDeadline(time.Now().Add(time.Nanosecond * 1000000))
				if err != nil {
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						continue
					} else {
						fmt.Println(err.Error())
						return
					}
				}

				scanner := bufio.NewScanner(c.Conn) // Start a new scanner
				query := ""                         // Client query variable

				// Read until ; or a single 'quit'
				for scanner.Scan() {
					err := scanner.Err()
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						continue
					} else if err == io.EOF {
						cursus.ConnectionQueueMu.Lock()
						delete(cursus.ConnectionQueue, c.Conn.RemoteAddr().String())
						cursus.ConnectionQueueMu.Unlock()
						continue
					} else {
						query += scanner.Text()
						query = strings.Join(strings.Fields(strings.TrimSpace(query)), " ")

						if strings.HasPrefix(scanner.Text(), "quit") {
							break
						} else if strings.HasSuffix(query, ";") {
							fmt.Println("QUERY:", query) // Log

							wg := &sync.WaitGroup{}
							mu := &sync.Mutex{}

							switch c.User["permission"] {
							case "R":
							case strings.HasPrefix(query, "update"):
								c.Text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
								continue
							case strings.HasPrefix(query, "insert"):
								c.Text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
								continue
							case strings.HasPrefix(query, "delete"):
								c.Text.PrintfLine(fmt.Sprintf("%d User not authorized", 4))
								continue
							case strings.HasPrefix(query, "select"):
								goto allowed
								continue
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
									c.Text.PrintfLine(fmt.Sprintf("%d Invalid query", 3000))
									query = "" // Clear query variable and listen for another
									continue
								}

								// Regex for insert i.e coll({}) in-between parenthesis
								var insertJsonRegex = regexp.MustCompile(`\((.*?)\)`)

								insertJson := insertJsonRegex.FindStringSubmatch(query) // Get insert JSON

								collection := strings.ReplaceAll(strings.Split(query, "({\"")[0], "insert into ", "")

								if len(insertJson) != 2 {
									c.Text.PrintfLine(fmt.Sprintf("%d Invalid query", 3000))
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
												c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
												query = ""
												continue
											}

											for _, a := range arr {
												body["values"] = append(body["values"].([]interface{}), a)
												body["keys"] = append(body["keys"].([]interface{}), strings.TrimSpace(strings.TrimSuffix(indx[1], "!"))) // add key for query
												body["oprs"] = append(body["oprs"].([]interface{}), "==")

											}

											cursus.NodesMu.Lock()
											res := cursus.QueryNodesRet(c, body, wg, mu)
											for _, r := range res {
												if !strings.EqualFold(r, "null") {
													result := make(map[string]interface{})
													result["statusCode"] = 4004
													result["message"] = fmt.Sprintf("Document already exists")

													r, _ := json.Marshal(result)
													c.Text.PrintfLine(string(r))
													query = ""
													goto cont
												}
											}
											cursus.NodesMu.Unlock()

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
													c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
													query = ""
													continue
												}

												body["values"].([]interface{})[0] = b
											} else if cursus.IsFloat(body["values"].([]interface{})[0].(string)) {

												f, err := strconv.ParseFloat(body["values"].([]interface{})[0].(string), 64)
												if err != nil {
													c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
													query = ""
													continue
												}

												body["values"].([]interface{})[0] = f
											} else if cursus.IsInt(body["values"].([]interface{})[0].(string)) {
												i, err := strconv.Atoi(body["values"].([]interface{})[0].(string))
												if err != nil {
													c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
													query = ""
													continue
												}

												body["values"].([]interface{})[0] = i

											}
											cursus.NodesMu.Lock()
											res := cursus.QueryNodesRet(c, body, wg, mu)
											cursus.NodesMu.Unlock()
											for _, r := range res {
												if !strings.EqualFold(r, "null") {
													result := make(map[string]interface{})
													result["statusCode"] = 4004
													result["message"] = fmt.Sprintf("Document already exists")

													r, _ := json.Marshal(result)
													c.Text.PrintfLine(string(r))
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

								cursus.NodesMu.Lock()
								res := cursus.QueryNodesRet(c, body, wg, mu)
								for _, r := range res {
									if !strings.EqualFold(r, "null") {
										cursus.NodesMu.Unlock()
										goto retry // $id already exists
									}
								}
								cursus.NodesMu.Unlock()

								goto insert
							retry:
								body["values"].([]interface{})[0] = uuid.New().String()
								cursus.NodesMu.Lock()
								res = cursus.QueryNodesRet(c, body, wg, mu)
								for _, r := range res {
									if !strings.EqualFold(r, "null") {
										cursus.NodesMu.Unlock()
										goto retry // $id already exists
									}
								}

								cursus.NodesMu.Unlock()
								goto insert

							insert:
								cursus.InsertIntoNode(c, strings.ReplaceAll(insertJson[1], "!\":", "\":"), collection, body["values"].([]interface{})[0].(string))

								query = ""
								continue
							case strings.HasPrefix(query, "select "):

								if !strings.Contains(query, "from ") {
									c.Text.PrintfLine(fmt.Sprintf("%d From is required", 4006))
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

									err := cursus.QueryNodes(c, body, wg, mu)
									if err != nil {
										c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
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
											c.Text.PrintfLine(fmt.Sprintf("%d Invalid query operator.", 4007))
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
												c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
												query = ""
												continue
											}

											body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = b
										} else if cursus.IsFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

											f, err := strconv.ParseFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), 64)
											if err != nil {
												c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
												query = ""
												continue
											}

											body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = f
										} else if cursus.IsInt(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {
											i, err := strconv.Atoi(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
											if err != nil {
												c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
												query = ""
												continue
											}

											body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = i

										}

									}

									err := cursus.QueryNodes(c, body, wg, mu)
									if err != nil {
										c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
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
										c.Text.PrintfLine(fmt.Sprintf("%d Set is missing =", 4008))
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
											c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
											query = ""
											continue
										}

										val = b
									} else if cursus.IsFloat(val.(string)) {

										f, err := strconv.ParseFloat(val.(string), 64)
										if err != nil {
											c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
											query = ""
											continue
										}

										val = f
									} else if cursus.IsInt(val.(string)) {
										i, err := strconv.Atoi(val.(string))
										if err != nil {
											c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
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
										c.Text.PrintfLine(fmt.Sprintf("%d Invalid query operator.", 4007))
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
											c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
											query = ""
											continue
										}

										val = b
									} else if cursus.IsFloat(val.(string)) {

										f, err := strconv.ParseFloat(val.(string), 64)
										if err != nil {
											c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
											query = ""
											continue
										}

										val = f
									} else if cursus.IsInt(val.(string)) {
										i, err := strconv.Atoi(val.(string))
										if err != nil {
											c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
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

								err := cursus.QueryNodes(c, body, wg, mu)
								if err != nil {
									c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
									query = ""
									continue
								}

								query = ""
								continue

							case strings.HasPrefix(query, "delete "):
								// delete 1 from users where name == 'alex' && last == 'padula';

								if !strings.Contains(query, "from ") {
									c.Text.PrintfLine(fmt.Sprintf("%d From is required", 4006))
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

									err := cursus.QueryNodes(c, body, wg, mu)
									if err != nil {
										c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
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
											c.Text.PrintfLine(fmt.Sprintf("%d Invalid query operator.", 4007))
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
												c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
												query = ""
												continue
											}

											body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = b
										} else if cursus.IsFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {

											f, err := strconv.ParseFloat(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string), 64)
											if err != nil {
												c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
												query = ""
												continue
											}

											body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = f
										} else if cursus.IsInt(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string)) {
											i, err := strconv.Atoi(body["values"].([]interface{})[len(body["values"].([]interface{}))-1].(string))
											if err != nil {
												c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
												query = ""
												continue
											}

											body["values"].([]interface{})[len(body["values"].([]interface{}))-1] = i

										}

									}

									err := cursus.QueryNodes(c, body, wg, mu)
									if err != nil {
										c.Text.PrintfLine(fmt.Sprintf("%d Unknown error %s", 500, err.Error()))
										query = ""
										continue
									}

									query = ""
									continue

								}
							case strings.HasPrefix(query, "delete user "):
								splQ := strings.Split(query, "delete user ")

								if len(splQ) != 2 {
									c.Text.PrintfLine("%d Invalid command/query.", 4005)
									query = ""
									continue
								}

								err := cursus.RemoveUser(splQ[1])
								if err != nil {
									c.Text.PrintfLine("%d Database user %s removed successfully.", 201, splQ[1])
									query = ""
									continue
								}

								c.Text.PrintfLine("%d No user exists with username %s.", 102, splQ[1])
								query = ""
								continue

							case strings.HasPrefix(query, "new user "): // new user username, password, RW
								splQ := strings.Split(query, "new user ")

								// now we split at comma if value is equal to 2
								if len(splQ) != 2 {
									c.Text.PrintfLine("%d Invalid command/query.", 4005)
									query = ""
									continue
								}

								splQComma := strings.Split(splQ[1], ",")

								if len(splQComma) != 3 {
									c.Text.PrintfLine("%d Invalid command/query.", 4005)
									query = ""
									continue
								}

								_, _, err = cursus.NewUser(strings.TrimSpace(splQComma[0]), strings.TrimSpace(splQComma[1]), strings.TrimSpace(splQComma[2]))
								if err != nil {
									c.Text.PrintfLine(err.Error())
									query = ""
									continue
								}

								c.Text.PrintfLine(fmt.Sprintf("%d New database user %s created successfully.", 200, strings.TrimSpace(splQComma[0])))
								query = ""
								continue

							default:
								c.Text.PrintfLine("%d Invalid command/query.", 4005)
								query = ""
								continue

							}

						}
					}
				}
			}
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
				cursus.SignalChannel <- os.Interrupt
				return
			}

			// Dial tcp address up
			conn, err := net.DialTCP("tcp", nil, tcpAddr)
			if err != nil {
				fmt.Println("ConnectToNodes():", err.Error())
				cursus.SignalChannel <- os.Interrupt
				return
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
				cursus.NodeConnections = append(cursus.NodeConnections, NodeConnection{
					Conn:       conn,
					SecureConn: secureConn,
					Text:       textproto.NewConn(secureConn),
				})

				// Report back successful connection
				fmt.Println("ConnectToNodes(): Node connection established to", conn.RemoteAddr().String())
			} else {
				// Report back invalid key.
				fmt.Println("ConnectToNodes():", "Invalid key.")
				cursus.SignalChannel <- os.Interrupt
				return
			}
		}
	} else {
		for _, n := range cursus.Config.Nodes {

			// Resolve TCP addr based on what's provided within n ie (0.0.0.0:p)
			tcpAddr, err := net.ResolveTCPAddr("tcp", n)
			if err != nil {
				fmt.Println("ConnectToNodes():", err.Error())
				cursus.SignalChannel <- os.Interrupt
				return
			}

			// Dial tcp address up
			conn, err := net.DialTCP("tcp", nil, tcpAddr)
			if err != nil {
				fmt.Println("ConnectToNodes():", err.Error())
				cursus.SignalChannel <- os.Interrupt
				return
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
				cursus.NodeConnections = append(cursus.NodeConnections, NodeConnection{
					Conn: conn,
					Text: textproto.NewConn(conn),
				})

				// Report back successful connection
				fmt.Println("ConnectToNodes(): Node connection established to", conn.RemoteAddr().String())
			} else {
				// Report back invalid key
				fmt.Println("ConnectToNodes():", "Invalid key.")
				cursus.SignalChannel <- os.Interrupt
				return
			}

		}

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

// SignalListener listeners for system signals are does a graceful shutdown
func (cursus *Cursus) SignalListener() {
	defer cursus.Wg.Done()
	for {
		select {
		case sig := <-cursus.SignalChannel:
			log.Println("received", sig)
			cursus.ContextCancel()

			return

		default:
			time.Sleep(time.Millisecond * 1)
		}
	}
}

// main CursusDB Cluster starts here
func main() {
	var cursus Cursus                                     // Main CursusDB Cluster variable
	cursus.SignalChannel = make(chan os.Signal, 1)        // Create channel for os signals
	cursus.Wg = &sync.WaitGroup{}                         // Cluster global waitgroup
	cursus.ConnectionQueue = make(map[string]*Connection) // Make connection queue [remote_addr_str]*Connection
	cursus.ConnectionQueueMu = &sync.RWMutex{}            // Cluster connection queue mutex
	cursus.ConnectionChannel = make(chan *Connection)
	cursus.Context, cursus.ContextCancel = context.WithCancel(context.Background())

	cursus.ConfigMu = &sync.RWMutex{} // Cluster config mutex

	// We check if a .cursusconfig file exists
	if _, err := os.Stat("./.cursusconfig"); errors.Is(err, os.ErrNotExist) {
		// .cursusconfig does not exist..

		cursus.Config.Port = 7681            // Default CursusDB cluster port
		cursus.Config.NodeReaderSize = 10240 // Default node reader size of 10240 bytes.. Pretty large json response
		cursus.Config.ConnectionQueueWorkers = 4
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

		// Marhsal config to yaml
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

		// Unmarhsal config into cluster.config
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

	cursus.NodesMu = &sync.Mutex{} // Cluster nodes mutex

	// If port provided as flag use it instead of whats on config file
	flag.IntVar(&cursus.Config.Port, "port", cursus.Config.Port, "port for cluster")
	flag.Parse()

	signal.Notify(cursus.SignalChannel, syscall.SIGINT, syscall.SIGTERM)

	cursus.ConnectToNodes() // Connect to all nodes

	cursus.Wg.Add(1)
	go cursus.SignalListener()

	cursus.Wg.Add(1)
	go cursus.StartTCP_TLSListener()

	cursus.Wg.Add(1)
	go cursus.ConnectionEventWorker()

	// Start up connection queue goroutines in which listen for events.
	for i := cursus.Config.ConnectionQueueWorkers; i > 0; i-- { // Get amount of workers based on config
		cursus.Wg.Add(1)
		go cursus.ConnectionEventLoop(i + 1)
	}

	cursus.Wg.Wait() // Wait for all go routines to finish

	os.Exit(0) // Shut down system on success

}
