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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"

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

type Cluster struct {
	Listener        net.Listener
	Wg              *sync.WaitGroup
	SignalChannel   chan os.Signal
	Config          Config
	NodeConnections []NodeConnection
	Connections     []*Connection
}

type Config struct {
	Nodes []string `yaml:"nodes"`
}

type NodeConnection struct {
	Conn *net.TCPConn
	Text *textproto.Conn
}

type Connection struct {
	Conn net.Conn
	Text *textproto.Conn
}

func (cluster *Cluster) TCP_TLSListener() {
	defer cluster.Wg.Done()
	var err error

	//cer, err := tls.LoadX509KeyPair("cert", "key")
	//if err != nil {
	//	panic("error loading cert: " + err.Error())
	//}
	//
	//config := &tls.Config{Certificates: []tls.Certificate{cer}}
	//tls.Listen("tcp", "0.0.0.0:7222", config)
	cluster.Listener, err = net.Listen("tcp", "0.0.0.0:7222")
	if err != nil {
		log.Println(err.Error())
		cluster.SignalChannel <- os.Interrupt
		return
	}

	for {
		conn, err := cluster.Listener.Accept()
		if err != nil {
			return
		}

		cluster.Wg.Add(1)
		go cluster.HandleConnection(&Connection{
			Conn: conn,
		})
	}

}

func (cluster *Cluster) IsString(str string) bool {
	switch {
	case strings.HasPrefix(str, "\"") && strings.HasSuffix(str, "\""):

		return true
	case strings.HasPrefix(str, "'") && strings.HasSuffix(str, "'"):
		return true
	default:
		return false
	}
}

func (cluster *Cluster) IsInt(str string) bool {
	if _, err := strconv.Atoi(str); err == nil {
		return true
	}

	return false
}

func (cluster *Cluster) IsFloat(str string) bool {
	if _, err := strconv.ParseFloat(str, 64); err == nil {
		return true
	}

	return false
}

func (cluster *Cluster) IsBool(str string) bool {
	if _, err := strconv.ParseBool(str); err == nil {
		return true
	}

	return false
}

func (cluster *Cluster) HandleConnection(connection *Connection) {
	defer cluster.Wg.Done()
	cluster.Connections = append(cluster.Connections, connection)
	connection.Text = textproto.NewConn(connection.Conn)
	defer connection.Text.Close()
	defer connection.Conn.Close()

	defer func(conn *Connection) {
		for i, c := range cluster.Connections {
			if c == conn {
				cluster.Connections = append(cluster.Connections[:i], cluster.Connections[i+1:]...)
			}
		}

	}(connection)

	scanner := bufio.NewScanner(connection.Conn)
	query := ""
	for scanner.Scan() {
		query += scanner.Text()
		query = strings.Join(strings.Fields(strings.TrimSpace(query)), " ")

		if strings.HasPrefix(scanner.Text(), "quit") {
			break
		} else if strings.HasSuffix(query, ";") {
			fmt.Println("QUERY:", query)

			wg := &sync.WaitGroup{}
			mu := &sync.Mutex{}

			switch {
			case strings.HasPrefix(query, "insert "):
				// insert into users({"firstName": "alex", "email": "apadula@postdaemon.com", "age": 23, "active": false});
				// insert into users({"firstName!": "alex", "email": "apadula@postdaemon.com", "age": 23, "active": false}); ! means only insert if non existent on all nodes.
				// update 1 in users to firstName! = 'alex'; // only if firstName value does not exist.

				if !strings.HasPrefix(query, "insert into ") {
					connection.Text.PrintfLine("Invalid query")
					query = ""
					continue
				}

				var rgx = regexp.MustCompile(`\((.*?)\)`)

				rs := rgx.FindStringSubmatch(query)

				collection := strings.ReplaceAll(strings.Split(query, "({\"")[0], "insert into ", "")

				if len(rs) != 2 {
					connection.Text.PrintfLine("Invalid query")
					query = ""
					continue
				}

				var indexed = regexp.MustCompile(`"([^"]+!)"`) // "email!":

				indexedRes := indexed.FindAllStringSubmatch(query, -1)
				for _, indx := range indexedRes {

					kValue := regexp.MustCompile(fmt.Sprintf(`%s"\s*:\s*(true|false|null|[A-Za-z]|[0-9]*[.]?[0-9]+|".*?"|'.*?')`, indx[1]))

					body := make(map[string]interface{})
					body["action"] = "select"
					body["limit"] = "1"
					body["collection"] = collection
					body["key"] = strings.TrimSpace(strings.TrimSuffix(indx[1], "!"))
					body["opr"] = "=="
					body["lock"] = true // lock on read.  There can be many clusters reading at one time.  This helps setup indexes across all nodes
					body["value"] = kValue.FindStringSubmatch(query)[1]

					if strings.EqualFold(body["value"].(string), "null") {
						body["value"] = nil
					} else if cluster.IsString(body["value"].(string)) {

						body["value"] = body["value"].(string)
						body["value"] = strings.TrimSuffix(body["value"].(string), "\"")
						body["value"] = strings.TrimPrefix(body["value"].(string), "\"")
						body["value"] = strings.TrimSuffix(body["value"].(string), "'")
						body["value"] = strings.TrimPrefix(body["value"].(string), "'")
					} else if cluster.IsBool(body["value"].(string)) {

						b, err := strconv.ParseBool(body["value"].(string))
						if err != nil {
							connection.Text.PrintfLine("Something went wrong. %s", err.Error())
							query = ""
							continue
						}

						body["value"] = b
					} else if cluster.IsFloat(body["value"].(string)) {

						f, err := strconv.ParseFloat(body["value"].(string), 64)
						if err != nil {
							connection.Text.PrintfLine("Something went wrong. %s", err.Error())
							query = ""
							continue
						}

						body["value"] = f
					} else if cluster.IsInt(body["value"].(string)) {
						i, err := strconv.Atoi(body["value"].(string))
						if err != nil {
							connection.Text.PrintfLine("Something went wrong. %s", err.Error())
							query = ""
							continue
						}

						body["value"] = i

					}

					res := cluster.QueryNodesRet(connection, body, wg, mu)
					for _, r := range res {
						if !strings.EqualFold(r, "null") {

							connection.Text.PrintfLine("Document already exists %s", strings.ReplaceAll(rs[1], "!\":", "\":"))
							query = ""
							goto cont
						}
					}
				}

				goto ok

			cont:
				continue

			ok:

				body := make(map[string]interface{})
				body["action"] = "select"
				body["limit"] = "1"
				body["collection"] = collection
				body["key"] = "$id"
				body["opr"] = "=="
				body["lock"] = true                 // lock on read.  There can be many clusters reading at one time.  This helps setup indexes across all nodes
				body["value"] = uuid.New().String() // $id

			retry:
				body["value"] = uuid.New().String() // $id

				res := cluster.QueryNodesRet(connection, body, wg, mu)
				for _, r := range res {
					if !strings.EqualFold(r, "null") {
						goto retry // $id already exists
					}
				}

				cluster.InsertIntoNode(connection, rs[1], collection, body["value"].(string))

				query = ""
				continue
			case strings.HasPrefix(query, "select "):
				// select * from users where firstName == "alex";
				// select 2 from users where age > 22;
				// select 22,2 from users where age > 22 && name == 'john' && createdOn >= 19992929;
				querySplit := strings.Split(strings.ReplaceAll(strings.Join(strings.Fields(strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(query, "where", ""), "from", ""))), " "), "from", ""), " ")

				if len(strings.Split(query, "&& ")) > 0 {
					log.Println("ANDs...")
					log.Println(strings.Split(query, "&& ")[2])
					continue
				} else if len(querySplit) != 6 {
					connection.Text.PrintfLine("Invalid query")
					query = ""
					continue
				}

				body := make(map[string]interface{})
				body["action"] = querySplit[0]
				body["limit"] = querySplit[1]
				body["collection"] = querySplit[2]
				body["keys"] = querySplit[3]
				body["opr"] = querySplit[4]
				body["lock"] = false // lock on read.  There can be many clusters reading at one time.

				switch {
				case strings.EqualFold(body["opr"].(string), "=="):
				case strings.EqualFold(body["opr"].(string), "!="):
				case strings.EqualFold(body["opr"].(string), "<="):
				case strings.EqualFold(body["opr"].(string), ">="):
				case strings.EqualFold(body["opr"].(string), "<"):
				case strings.EqualFold(body["opr"].(string), ">"):
				default:
					connection.Text.PrintfLine("Invalid query operator.")
					query = ""
					continue
				}

				body["value"] = strings.TrimSuffix(querySplit[5], ";")

				if strings.EqualFold(body["value"].(string), "null") {
					body["value"] = nil
				} else if cluster.IsString(body["value"].(string)) {

					body["value"] = body["value"].(string)
					body["value"] = strings.TrimSuffix(body["value"].(string), "\"")
					body["value"] = strings.TrimPrefix(body["value"].(string), "\"")
					body["value"] = strings.TrimSuffix(body["value"].(string), "'")
					body["value"] = strings.TrimPrefix(body["value"].(string), "'")
				} else if cluster.IsBool(body["value"].(string)) {

					b, err := strconv.ParseBool(body["value"].(string))
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}

					body["value"] = b
				} else if cluster.IsFloat(body["value"].(string)) {

					f, err := strconv.ParseFloat(body["value"].(string), 64)
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}

					body["value"] = f
				} else if cluster.IsInt(body["value"].(string)) {
					i, err := strconv.Atoi(body["value"].(string))
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}

					body["value"] = i

				}

				err := cluster.QueryNodes(connection, body, wg, mu)
				if err != nil {
					connection.Text.PrintfLine("Something went wrong. %s", err.Error())
					query = ""
					continue
				}

				query = ""
				continue
			case strings.HasPrefix(query, "update "):
				// update * in users where firstName == "alex" to = "daniel";
				// update 2 in users where age > 22; -- gets the last 2 inserted users documents where age is > 22!
				// update 22,2 in users where age > 22 to = 23;
				// select * in users where firstName == "alex"  to = "daniel";
				// select 2 in users where age > 22 to = 23;
				// select 22,2 in users where age > 22 to = 23;
				querySplit := strings.Split(strings.ReplaceAll(strings.Join(strings.Fields(strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(query, "where", ""), "in", ""))), " "), "from", ""), " ")

				if len(querySplit) != 9 {
					connection.Text.PrintfLine("Invalid query")
					query = ""
					continue
				}

				if querySplit[6] != "to" && querySplit[7] != "=" {
					connection.Text.PrintfLine("Invalid query")
					query = ""
					continue
				}

				body := make(map[string]interface{})
				body["action"] = querySplit[0]
				body["limit"] = querySplit[1]
				body["collection"] = querySplit[2]
				body["key"] = querySplit[3]
				body["opr"] = querySplit[4]
				body["value"] = querySplit[5]
				body["lock"] = false // lock on read.  There can be many clusters reading at one time.
				body["to"] = strings.TrimSuffix(querySplit[8], ";")

				switch {
				case strings.EqualFold(body["opr"].(string), "=="):
				case strings.EqualFold(body["opr"].(string), "!="):
				case strings.EqualFold(body["opr"].(string), "<="):
				case strings.EqualFold(body["opr"].(string), ">="):
				case strings.EqualFold(body["opr"].(string), "<"):
				case strings.EqualFold(body["opr"].(string), ">"):
				default:
					connection.Text.PrintfLine("Invalid query operator.")
					query = ""
					continue
				}

				if strings.EqualFold(body["value"].(string), "null") {
					body["value"] = nil
				} else if cluster.IsString(body["value"].(string)) {

					body["value"] = body["value"].(string)
					body["value"] = strings.TrimSuffix(body["value"].(string), "\"")
					body["value"] = strings.TrimPrefix(body["value"].(string), "\"")
					body["value"] = strings.TrimSuffix(body["value"].(string), "'")
					body["value"] = strings.TrimPrefix(body["value"].(string), "'")

				} else if cluster.IsBool(body["value"].(string)) {

					b, err := strconv.ParseBool(body["value"].(string))
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}

					body["value"] = b
				} else if cluster.IsFloat(body["value"].(string)) {

					f, err := strconv.ParseFloat(body["value"].(string), 64)
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}

					body["value"] = f
				} else if cluster.IsInt(body["value"].(string)) {
					i, err := strconv.Atoi(body["value"].(string))
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}

					body["value"] = i

				}

				if strings.EqualFold(body["to"].(string), "null") {
					body["to"] = nil
				} else if cluster.IsString(body["to"].(string)) {

					body["to"] = body["to"].(string)
					body["to"] = strings.TrimSuffix(body["to"].(string), "\"")
					body["to"] = strings.TrimPrefix(body["to"].(string), "\"")
					body["to"] = strings.TrimSuffix(body["to"].(string), "'")
					body["to"] = strings.TrimPrefix(body["to"].(string), "'")

				} else if cluster.IsBool(body["to"].(string)) {

					b, err := strconv.ParseBool(body["to"].(string))
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}

					body["to"] = b
				} else if cluster.IsFloat(body["to"].(string)) {

					f, err := strconv.ParseFloat(body["to"].(string), 64)
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}

					body["to"] = f
				} else if cluster.IsInt(body["to"].(string)) {
					i, err := strconv.Atoi(body["to"].(string))
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}

					body["to"] = i

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

				// delete * from users where firstName == "alex";
				// delete 2 from users where age > 22; -- gets the last 2 inserted users documents where age is > 22!
				// delete 22,2 from users where age > 22;
				// select * from users where firstName == "alex";
				// select 2 from users where age > 22;
				// select 22,2 from users where age > 22;
				querySplit := strings.Split(strings.ReplaceAll(strings.Join(strings.Fields(strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(query, "where", ""), "from", ""))), " "), "from", ""), " ")

				if len(querySplit) != 6 {
					connection.Text.PrintfLine("Invalid query")
					query = ""
					continue
				}

				body := make(map[string]interface{})
				body["action"] = querySplit[0]
				body["limit"] = querySplit[1]
				body["collection"] = querySplit[2]
				body["key"] = querySplit[3]
				body["opr"] = querySplit[4]
				body["lock"] = false // lock on read.  There can be many clusters reading at one time.

				switch {
				case strings.EqualFold(body["opr"].(string), "=="):
				case strings.EqualFold(body["opr"].(string), "!="):
				case strings.EqualFold(body["opr"].(string), "<="):
				case strings.EqualFold(body["opr"].(string), ">="):
				case strings.EqualFold(body["opr"].(string), "<"):
				case strings.EqualFold(body["opr"].(string), ">"):
				default:
					connection.Text.PrintfLine("Invalid query operator.")
					query = ""
					continue
				}

				body["value"] = strings.TrimSuffix(querySplit[5], ";")
				if strings.EqualFold(body["value"].(string), "null") {
					body["value"] = nil
				} else if cluster.IsString(strings.TrimSuffix(querySplit[5], ";")) {

					body["value"] = body["value"].(string)
					body["value"] = strings.TrimSuffix(body["value"].(string), "\"")
					body["value"] = strings.TrimPrefix(body["value"].(string), "\"")
					body["value"] = strings.TrimSuffix(body["value"].(string), "'")
					body["value"] = strings.TrimPrefix(body["value"].(string), "'")
				} else if cluster.IsBool(body["value"].(string)) {

					b, err := strconv.ParseBool(body["value"].(string))
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}

					body["value"] = b
				} else if cluster.IsFloat(body["value"].(string)) {

					f, err := strconv.ParseFloat(body["value"].(string), 64)
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}

					body["value"] = f
				} else if cluster.IsInt(body["value"].(string)) {
					i, err := strconv.Atoi(body["value"].(string))
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}

					body["value"] = i

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
	data := make(map[string]interface{})
	err := json.Unmarshal([]byte(insert), &data)
	if err != nil {
		connection.Text.PrintfLine("Cannot insert. %s", err.Error())
		return
	}

	data["$id"] = id
	jsonStringData, err := json.Marshal(data)
	if err != nil {
		connection.Text.PrintfLine("Cannot insert. %s", err.Error())
		return
	}

	jsonMap := make(map[string]interface{})

	jsonMap["document"] = string(jsonStringData)
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

			cluster.Listener.Close()
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
			println("ResolveTCPAddr failed:", err.Error())
			os.Exit(1)
		}

		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			println("Dial failed:", err.Error())
			os.Exit(1)
		}

		cluster.NodeConnections = append(cluster.NodeConnections, NodeConnection{
			Conn: conn,
			Text: textproto.NewConn(conn),
		})
	}
}

func main() {
	var cluster Cluster

	cluster.Config.Nodes = append(cluster.Config.Nodes, "232.232.22")
	cluster.Config.Nodes = append(cluster.Config.Nodes, "2332.2332.22")
	if _, err := os.Stat("./.clusterconfig"); errors.Is(err, os.ErrNotExist) {
		clusterConfigFile, err := os.OpenFile("./.clusterconfig", os.O_CREATE|os.O_RDWR, 0777)
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
		clusterConfigFile, err := os.ReadFile("./.clusterconfig")
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		err = yaml.Unmarshal(clusterConfigFile, &cluster.Config)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		log.Println(cluster.Config)
	}

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
