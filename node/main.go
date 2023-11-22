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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/textproto"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Node struct {
	Listener      net.Listener
	Wg            *sync.WaitGroup
	SignalChannel chan os.Signal
	Connections   []*Connection
	Data          Data
}

type Data struct {
	Map     map[string][]map[string]interface{}
	Writers map[string]*sync.RWMutex
}

type Connection struct {
	Conn net.Conn
	Text *textproto.Conn
}

func (n *Node) TCP_TLSListener(port int) {
	defer n.Wg.Done()
	var err error

	//cer, err := tls.LoadX509KeyPair("cert", "key")
	//if err != nil {
	//	panic("error loading cert: " + err.Error())
	//}
	//
	//config := &tls.Config{Certificates: []tls.Certificate{cer}}
	//tls.Listen("tcp", "0.0.0.0:7222", config)
	n.Listener, err = net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		log.Println(err.Error())
		n.SignalChannel <- os.Interrupt
		return
	}

	for {
		conn, err := n.Listener.Accept()
		if err != nil {
			return
		}

		n.Wg.Add(1)
		go n.HandleConnection(&Connection{
			Conn: conn,
		})
	}

}

func (n *Node) update(collection, k string, v interface{}, nv interface{}, vol int, opr string) []interface{} {
	var objects []interface{}
	for _, d := range n.Data.Map[collection] {
		if opr == "" {
			fmt.Sprintf("Query operator required.")
			return nil
		}

		if vol != -1 {
			if len(objects) == vol {
				return objects
			}
		}

		vType := fmt.Sprintf("%T", v)

		_, ok := d[k]
		if ok {

			if d[k] == nil {
				n.Data.Writers[collection].Lock()
				d[k] = nv
				n.Data.Writers[collection].Unlock()
				objects = append(objects, d)
				continue
			}

			if reflect.TypeOf(d[k]).Kind() == reflect.Slice {
				for i, dd := range d[k].([]interface{}) {

					if len(objects) == vol {
						return objects
					}

					if reflect.TypeOf(dd).Kind() == reflect.Float64 {
						if vType == "int" {
							var interfaceI int = int(dd.(float64))

							if opr == "==" {
								if reflect.DeepEqual(interfaceI, v) {
									n.Data.Writers[collection].Lock()
									d[k].([]interface{})[i] = nv
									n.Data.Writers[collection].Unlock()
									objects = append(objects, d)
								}
							} else if opr == "!=" {
								if !reflect.DeepEqual(interfaceI, v) {
									n.Data.Writers[collection].Lock()
									d[k].([]interface{})[i] = nv
									n.Data.Writers[collection].Unlock()
									objects = append(objects, d)
								}
							} else if opr == ">" {
								if vType == "int" {
									if interfaceI > v.(int) {
										n.Data.Writers[collection].Lock()
										d[k].([]interface{})[i] = nv
										n.Data.Writers[collection].Unlock()
										objects = append(objects, d)
									}
								}
							} else if opr == "<" {
								if vType == "int" {
									if interfaceI < v.(int) {
										n.Data.Writers[collection].Lock()
										d[k].([]interface{})[i] = nv
										n.Data.Writers[collection].Unlock()
										objects = append(objects, d)
									}
								}
							} else if opr == ">=" {
								if vType == "int" {
									if interfaceI >= v.(int) {
										n.Data.Writers[collection].Lock()
										d[k].([]interface{})[i] = nv
										n.Data.Writers[collection].Unlock()
										objects = append(objects, d)
									}
								}
							} else if opr == "<=" {
								if vType == "int" {
									if interfaceI <= v.(int) {
										n.Data.Writers[collection].Lock()
										d[k].([]interface{})[i] = nv
										n.Data.Writers[collection].Unlock()
										objects = append(objects, d)
									}
								}
							}
						} else if vType == "float64" {
							var interfaceI float64 = d[k].(float64)

							if opr == "==" {
								if reflect.DeepEqual(interfaceI, v) {
									n.Data.Writers[collection].Lock()
									d[k].([]interface{})[i] = nv
									n.Data.Writers[collection].Unlock()
									objects = append(objects, d)
								}
							} else if opr == "!=" {
								if !reflect.DeepEqual(interfaceI, v) {
									n.Data.Writers[collection].Lock()
									d[k].([]interface{})[i] = nv
									n.Data.Writers[collection].Unlock()
									objects = append(objects, d)
								}
							} else if opr == ">" {
								if float64(interfaceI) > v.(float64) {
									n.Data.Writers[collection].Lock()
									d[k].([]interface{})[i] = nv
									n.Data.Writers[collection].Unlock()
									objects = append(objects, d)
								}

							} else if opr == "<" {
								if float64(interfaceI) < v.(float64) {
									n.Data.Writers[collection].Lock()
									d[k].([]interface{})[i] = nv
									n.Data.Writers[collection].Unlock()
									objects = append(objects, d)
								}

							} else if opr == ">=" {

								if float64(interfaceI) >= v.(float64) {
									n.Data.Writers[collection].Lock()
									d[k].([]interface{})[i] = nv
									n.Data.Writers[collection].Unlock()
									objects = append(objects, d)
								}

							} else if opr == "<=" {
								if float64(interfaceI) <= v.(float64) {
									n.Data.Writers[collection].Lock()
									d[k].([]interface{})[i] = nv
									n.Data.Writers[collection].Unlock()
									objects = append(objects, d)
								}

							}
						}
					} else if reflect.TypeOf(dd).Kind() == reflect.Map {
						//for kkk, ddd := range dd.(map[string]interface{}) {
						//	// unimplemented
						//}
					} else {
						if opr == "==" {
							if reflect.DeepEqual(dd, v) {
								n.Data.Writers[collection].Lock()
								d[k].([]interface{})[i] = nv
								n.Data.Writers[collection].Unlock()
								objects = append(objects, d)
							}
						} else if opr == "!=" {
							if !reflect.DeepEqual(dd, v) {
								n.Data.Writers[collection].Lock()
								d[k].([]interface{})[i] = nv
								n.Data.Writers[collection].Unlock()
								objects = append(objects, d)
							}
						}
					}

				}
			} else if vType == "int" {
				var interfaceI int = int(d[k].(float64))

				if opr == "==" {
					if reflect.DeepEqual(interfaceI, v) {
						n.Data.Writers[collection].Lock()
						d[k] = nv
						n.Data.Writers[collection].Unlock()
						objects = append(objects, d)
					}
				} else if opr == "!=" {
					if !reflect.DeepEqual(interfaceI, v) {
						n.Data.Writers[collection].Lock()
						d[k] = nv
						n.Data.Writers[collection].Unlock()
						objects = append(objects, d)
					}
				} else if opr == ">" {
					if vType == "int" {
						if interfaceI > v.(int) {
							n.Data.Writers[collection].Lock()
							d[k] = nv
							n.Data.Writers[collection].Unlock()
							objects = append(objects, d)
						}
					}
				} else if opr == "<" {
					if vType == "int" {
						if interfaceI < v.(int) {
							n.Data.Writers[collection].Lock()
							d[k] = nv
							n.Data.Writers[collection].Unlock()
							objects = append(objects, d)
						}
					}
				} else if opr == ">=" {
					if vType == "int" {
						if interfaceI >= v.(int) {
							n.Data.Writers[collection].Lock()
							d[k] = nv
							n.Data.Writers[collection].Unlock()
							objects = append(objects, d)
						}
					}
				} else if opr == "<=" {
					if vType == "int" {
						if interfaceI <= v.(int) {
							n.Data.Writers[collection].Lock()
							d[k] = nv
							n.Data.Writers[collection].Unlock()
							objects = append(objects, d)
						}
					}
				}
			} else if vType == "float64" {
				var interfaceI float64 = d[k].(float64)

				if opr == "==" {
					if reflect.DeepEqual(interfaceI, v) {
						n.Data.Writers[collection].Lock()
						d[k] = nv
						n.Data.Writers[collection].Unlock()
						objects = append(objects, d)
					}
				} else if opr == "!=" {
					if !reflect.DeepEqual(interfaceI, v) {
						n.Data.Writers[collection].Lock()
						d[k] = nv
						n.Data.Writers[collection].Unlock()
						objects = append(objects, d)
					}
				} else if opr == ">" {
					if float64(interfaceI) > v.(float64) {
						n.Data.Writers[collection].Lock()
						d[k] = nv
						n.Data.Writers[collection].Unlock()
						objects = append(objects, d)
					}

				} else if opr == "<" {
					if float64(interfaceI) < v.(float64) {
						n.Data.Writers[collection].Lock()
						d[k] = nv
						n.Data.Writers[collection].Unlock()
						objects = append(objects, d)
					}

				} else if opr == ">=" {
					if float64(interfaceI) >= v.(float64) {
						n.Data.Writers[collection].Lock()
						d[k] = nv
						n.Data.Writers[collection].Unlock()
						objects = append(objects, d)
					}

				} else if opr == "<=" {
					if float64(interfaceI) <= v.(float64) {
						n.Data.Writers[collection].Lock()
						d[k] = nv
						n.Data.Writers[collection].Unlock()
						objects = append(objects, d)
					}

				}
			} else {
				if opr == "==" {
					if reflect.DeepEqual(d[k], v) {
						n.Data.Writers[collection].Lock()
						d[k] = nv
						n.Data.Writers[collection].Unlock()
						objects = append(objects, d)
					}
				} else if opr == "!=" {
					if !reflect.DeepEqual(d[k], v) {
						n.Data.Writers[collection].Lock()
						d[k] = nv
						n.Data.Writers[collection].Unlock()
						objects = append(objects, d)
					}
				}
			}
		}

	}

	return objects
}

func (n *Node) sel(collection string, k interface{}, v interface{}, vol int, skip int, opr interface{}, lock bool) []interface{} {

	if lock {
		writeMu, ok := n.Data.Writers[collection]
		if ok {
			writeMu.Lock()
			defer writeMu.Unlock()
		}
	}

	var objects []interface{}
	for i, d := range n.Data.Map[collection] {
		if k == nil && v == nil && opr == nil {
			if skip != 0 {
				skip = skip - 1
				continue
			}

			if vol != -1 {
				if i-1 == vol-1 {
					return objects
				}
			}
			objects = append(objects, d)
		} else {

			if opr == "" {
				fmt.Sprintf("Query operator required.")
				return nil
			}

			if skip != 0 {
				skip = skip - 1
				continue
			}

			if vol != -1 {
				if len(objects) == vol {
					return objects
				}
			}

			vType := fmt.Sprintf("%T", v)

			_, ok := d[k.(string)]
			if ok {

				if d[k.(string)] == nil {
					objects = append(objects, d)
					continue
				}

				if reflect.TypeOf(d[k.(string)]).Kind() == reflect.Slice {
					for _, dd := range d[k.(string)].([]interface{}) {

						if len(objects) == vol {
							return objects
						}

						if reflect.TypeOf(dd).Kind() == reflect.Float64 {
							if vType == "int" {
								var interfaceI int = int(dd.(float64))

								if opr == "==" {
									if reflect.DeepEqual(interfaceI, v) {
										objects = append(objects, d)
									}
								} else if opr == "!=" {
									if !reflect.DeepEqual(interfaceI, v) {
										objects = append(objects, d)
									}
								} else if opr == ">" {
									if vType == "int" {
										if interfaceI > v.(int) {
											objects = append(objects, d)
										}
									}
								} else if opr == "<" {
									if vType == "int" {
										if interfaceI < v.(int) {
											objects = append(objects, d)
										}
									}
								} else if opr == ">=" {
									if vType == "int" {
										if interfaceI >= v.(int) {
											objects = append(objects, d)
										}
									}
								} else if opr == "<=" {
									if vType == "int" {
										if interfaceI <= v.(int) {
											objects = append(objects, d)
										}
									}
								}
							} else if vType == "float64" {
								var interfaceI float64 = d[k.(string)].(float64)

								if opr == "==" {
									if reflect.DeepEqual(interfaceI, v) {
										objects = append(objects, d)
									}
								} else if opr == "!=" {
									if !reflect.DeepEqual(interfaceI, v) {
										objects = append(objects, d)
									}
								} else if opr == ">" {
									if float64(interfaceI) > v.(float64) {
										objects = append(objects, d)
									}

								} else if opr == "<" {
									if float64(interfaceI) < v.(float64) {
										objects = append(objects, d)
									}

								} else if opr == ">=" {

									if float64(interfaceI) >= v.(float64) {
										objects = append(objects, d)
									}

								} else if opr == "<=" {
									if float64(interfaceI) <= v.(float64) {
										objects = append(objects, d)
									}

								}
							}
						} else if reflect.TypeOf(dd).Kind() == reflect.Map {
							//for kkk, ddd := range dd.(map[string]interface{}) {
							//	// unimplemented
							//}
						} else {
							if opr == "==" {
								if reflect.DeepEqual(dd, v) {
									objects = append(objects, d)
								}
							} else if opr == "!=" {
								if !reflect.DeepEqual(dd, v) {
									objects = append(objects, d)
								}
							}
						}

					}
				} else if vType == "int" {
					var interfaceI int = int(d[k.(string)].(float64))

					if opr == "==" {
						if reflect.DeepEqual(interfaceI, v) {
							objects = append(objects, d)
						}
					} else if opr == "!=" {
						if !reflect.DeepEqual(interfaceI, v) {
							objects = append(objects, d)
						}
					} else if opr == ">" {
						if vType == "int" {
							if interfaceI > v.(int) {
								objects = append(objects, d)
							}
						}
					} else if opr == "<" {
						if vType == "int" {
							if interfaceI < v.(int) {
								objects = append(objects, d)
							}
						}
					} else if opr == ">=" {
						if vType == "int" {
							if interfaceI >= v.(int) {
								objects = append(objects, d)
							}
						}
					} else if opr == "<=" {
						if vType == "int" {
							if interfaceI <= v.(int) {
								objects = append(objects, d)
							}
						}
					}
				} else if vType == "float64" {
					var interfaceI float64 = d[k.(string)].(float64)

					if opr == "==" {
						if reflect.DeepEqual(interfaceI, v) {
							objects = append(objects, d)
						}
					} else if opr == "!=" {
						if !reflect.DeepEqual(interfaceI, v) {
							objects = append(objects, d)
						}
					} else if opr == ">" {
						if float64(interfaceI) > v.(float64) {
							objects = append(objects, d)
						}

					} else if opr == "<" {
						if float64(interfaceI) < v.(float64) {
							objects = append(objects, d)
						}

					} else if opr == ">=" {

						if float64(interfaceI) >= v.(float64) {
							objects = append(objects, d)
						}

					} else if opr == "<=" {
						if float64(interfaceI) <= v.(float64) {
							objects = append(objects, d)
						}

					}
				} else {
					if opr == "==" {
						if reflect.DeepEqual(d[k.(string)], v) {
							objects = append(objects, d)
						}
					} else if opr == "!=" {
						if !reflect.DeepEqual(d[k.(string)], v) {
							objects = append(objects, d)
						}
					}

				}
			}
		}

	}

	return objects
}

func (n *Node) insert(collection string, jsonMap map[string]interface{}, connection *Connection) error {
	jsonStr, err := json.Marshal(jsonMap)
	if err != nil {
		return err
	}

	if strings.Contains(string(jsonStr), "[{\"") {
		return errors.New("nested JSON objects not permitted")
	} else if strings.Contains(string(jsonStr), ": {\"") {
		return errors.New("nested JSON objects not permitted")
	} else if strings.Contains(string(jsonStr), ":{\"") {
		return errors.New("nested JSON objects not permitted")
	}

	doc := make(map[string]interface{})
	err = json.Unmarshal([]byte(jsonStr), &doc)
	if err != nil {
		return err
	}
	writeMu, ok := n.Data.Writers[collection]
	if ok {
		writeMu.Lock()
		defer writeMu.Unlock()

		n.Data.Map[collection] = append(n.Data.Map[collection], doc)
	} else {
		n.Data.Writers[collection] = &sync.RWMutex{}
		n.Data.Map[collection] = append(n.Data.Map[collection], doc)
	}

	response := make(map[string]interface{})
	response["statusCode"] = 2000
	response["message"] = "Document inserted"

	response["insert"] = doc

	responseMap, err := json.Marshal(response)
	if err != nil {
		return err
	}

	connection.Text.PrintfLine(string(responseMap))

	return nil
}

func (n *Node) HandleConnection(connection *Connection) {
	defer n.Wg.Done()
	n.Connections = append(n.Connections, connection)
	connection.Text = textproto.NewConn(connection.Conn)
	defer connection.Text.Close()
	defer connection.Conn.Close()

	defer func(conn *Connection) {
		for i, c := range n.Connections {
			if c == conn {
				n.Connections = append(n.Connections[:i], n.Connections[i+1:]...)
			}
		}

	}(connection)

	scanner := bufio.NewScanner(connection.Conn)

	for scanner.Scan() {
		query := scanner.Text()

		result := make(map[string]interface{})

		err := json.Unmarshal([]byte(query), &result)
		if err != nil {
			result["statusCode"] = 4000
			result["message"] = "Unmarshalable JSON"
			r, _ := json.Marshal(result)
			connection.Text.PrintfLine(string(r))
			continue
		}

		result["skip"] = 0

		action, ok := result["action"]
		if ok {
			switch {
			case strings.EqualFold(action.(string), "select"):

				if result["limit"].(string) == "*" {
					result["limit"] = -1
				} else if strings.Contains(result["limit"].(string), ",") {
					if len(strings.Split(result["limit"].(string), ",")) == 2 {
						result["skip"], err = strconv.Atoi(strings.Split(result["limit"].(string), ",")[0])
						if err != nil {
							connection.Text.PrintfLine("Limit skip must be an integer. %s", err.Error())
							query = ""
							continue
						}

						if !strings.EqualFold(strings.Split(result["limit"].(string), ",")[1], "*") {
							result["limit"], err = strconv.Atoi(strings.Split(result["limit"].(string), ",")[1])
							if err != nil {
								connection.Text.PrintfLine("Something went wrong. %s", err.Error())
								query = ""
								continue
							}
						} else {
							result["limit"] = -1
						}
					} else {
						connection.Text.PrintfLine("Invalid limiting value.")
						query = ""
						continue
					}
				} else {
					result["limit"], err = strconv.Atoi(result["limit"].(string))
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}
				}

				results := n.sel(result["collection"].(string), result["key"], result["value"], result["limit"].(int), result["skip"].(int), result["opr"], result["lock"].(bool))
				r, _ := json.Marshal(results)
				connection.Text.PrintfLine(string(r))
				continue
			case strings.EqualFold(action.(string), "update"):

				if result["limit"].(string) == "*" {
					result["limit"] = -1
				} else {
					result["limit"], err = strconv.Atoi(result["limit"].(string))
					if err != nil {
						connection.Text.PrintfLine("Something went wrong. %s", err.Error())
						query = ""
						continue
					}
				}

				results := n.update(result["collection"].(string), result["key"].(string), result["value"], result["to"], result["limit"].(int), result["opr"].(string))
				r, _ := json.Marshal(results)

				delete(result, "document")
				delete(result, "collection")
				delete(result, "action")
				delete(result, "key")
				delete(result, "limit")
				delete(result, "opr")
				delete(result, "value")
				delete(result, "to")
				delete(result, "skip")

				result["statusCode"] = 2000

				if reflect.DeepEqual(results, nil) || len(results) == 0 {
					result["message"] = "No documents updated."
				} else {
					result["message"] = "Document(s) updated."
				}

				result["updated"] = results
				r, _ = json.Marshal(result)

				connection.Text.PrintfLine(string(r))
				continue
			case strings.EqualFold(action.(string), "insert"):

				collection := result["collection"]
				doc := result["document"]
				delete(result, "document")
				delete(result, "collection")
				delete(result, "action")
				delete(result, "skip")

				err := n.insert(collection.(string), doc.(map[string]interface{}), connection)
				if err != nil {
					// Only error returned is a 4003 which means cannot insert nested object
					result["statusCode"] = 4003
					result["message"] = err.Error()
					r, _ := json.Marshal(result)
					connection.Text.PrintfLine(string(r))
					continue
				}

				continue
			default:

				result["statusCode"] = 4002
				result["message"] = "Invalid/Non-existent action"
				r, _ := json.Marshal(result)

				connection.Text.PrintfLine(string(r))
				continue
			}
		} else {
			result["statusCode"] = 4001
			result["message"] = "Missing action"
			r, _ := json.Marshal(result)

			connection.Text.PrintfLine(string(r))
			continue
		}

	}

}

func (n *Node) SignalListener() {
	defer n.Wg.Done()

	for {
		select {
		case sig := <-n.SignalChannel:
			log.Println("received", sig)
			log.Println("closing", len(n.Connections), "connections")
			for _, c := range n.Connections {
				c.Text.Close()
				c.Conn.Close()
			}

			n.Listener.Close()
			return
		default:
			time.Sleep(time.Millisecond * 125)
		}
	}
}

func main() {
	var node Node
	node.Data.Map = make(map[string][]map[string]interface{})
	node.Data.Writers = make(map[string]*sync.RWMutex)

	var port int
	flag.IntVar(&port, "port", 7222, "port for node")
	flag.Parse()

	node.SignalChannel = make(chan os.Signal, 1)

	signal.Notify(node.SignalChannel, syscall.SIGINT, syscall.SIGTERM)
	node.Wg = &sync.WaitGroup{}

	node.Wg.Add(1)
	go node.SignalListener()

	node.Wg.Add(1)
	go node.TCP_TLSListener(port)

	node.Wg.Wait()

}
