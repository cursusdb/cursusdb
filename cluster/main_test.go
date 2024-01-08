/*
* CursusDB
* Cluster Tests
* ******************************************************************
* These tests go through different methods within each system.  Furthermore the tests from this point on will be added to within this file if need be for general functions.
* More complex testing shall be done from this file onward on a wider scope either with specialized tooling or manually.
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
	"context"
	"crypto/tls"
	"io"
	"net"
	"os"
	"reflect"
	"sync"
	"testing"
)

func TestCursus_AuthenticateUser(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	type args struct {
		username string
		password string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		want1   map[string]interface{}
		wantErr bool
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			got, got1, err := cursus.AuthenticateUser(tt.args.username, tt.args.password)
			if (err != nil) != tt.wantErr {
				t.Errorf("AuthenticateUser() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("AuthenticateUser() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("AuthenticateUser() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestCursus_ConnectToNodes(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			cursus.ConnectToNodes()
		})
	}
}

func TestCursus_CountLog(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	type args struct {
		r io.Reader
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			if got := cursus.CountLog(tt.args.r); got != tt.want {
				t.Errorf("CountLog() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCursus_HandleClientConnection(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	type args struct {
		conn net.Conn
		user map[string]interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			cursus.HandleClientConnection(tt.args.conn, tt.args.user)
		})
	}
}

func TestCursus_InsertIntoNode(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	type args struct {
		connection *Connection
		insert     string
		collection string
		id         string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			cursus.InsertIntoNode(tt.args.connection, tt.args.insert, tt.args.collection, tt.args.id)
		})
	}
}

func TestCursus_IsBool(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	type args struct {
		str string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			if got := cursus.IsBool(tt.args.str); got != tt.want {
				t.Errorf("IsBool() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCursus_IsFloat(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	type args struct {
		str string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			if got := cursus.IsFloat(tt.args.str); got != tt.want {
				t.Errorf("IsFloat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCursus_IsInt(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	type args struct {
		str string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			if got := cursus.IsInt(tt.args.str); got != tt.want {
				t.Errorf("IsInt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCursus_IsString(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	type args struct {
		str string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			if got := cursus.IsString(tt.args.str); got != tt.want {
				t.Errorf("IsString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCursus_LostReconnect(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			cursus.LostReconnect()
		})
	}
}

func TestCursus_NewUser(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	type args struct {
		username   string
		password   string
		permission string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		want1   map[string]interface{}
		wantErr bool
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			got, got1, err := cursus.NewUser(tt.args.username, tt.args.password, tt.args.permission)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewUser() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("NewUser() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("NewUser() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestCursus_Printl(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	type args struct {
		data  string
		level string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			cursus.Printl(tt.args.data, tt.args.level)
		})
	}
}

func TestCursus_QueryNode(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	type args struct {
		n         *NodeConnection
		body      []byte
		wg        *sync.WaitGroup
		mu        *sync.RWMutex
		responses *map[string]string
		action    string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			cursus.QueryNode(tt.args.n, tt.args.body, tt.args.wg, tt.args.mu, tt.args.responses, tt.args.action)
		})
	}
}

func TestCursus_QueryNodes(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	type args struct {
		connection *Connection
		body       map[string]interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			if err := cursus.QueryNodes(tt.args.connection, tt.args.body); (err != nil) != tt.wantErr {
				t.Errorf("QueryNodes() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCursus_QueryNodesRet(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	type args struct {
		body map[string]interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[string]string
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			if got := cursus.QueryNodesRet(tt.args.body); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("QueryNodesRet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCursus_RemoveUser(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	type args struct {
		username string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			if err := cursus.RemoveUser(tt.args.username); (err != nil) != tt.wantErr {
				t.Errorf("RemoveUser() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCursus_RenewClusterConfig(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			if err := cursus.RenewClusterConfig(); (err != nil) != tt.wantErr {
				t.Errorf("RenewClusterConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCursus_SaveConfig(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			cursus.SaveConfig()
		})
	}
}

func TestCursus_SetupClusterConfig(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{wantErr: false},
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			if err := cursus.SetupClusterConfig(); (err != nil) != tt.wantErr {
				t.Errorf("SetupClusterConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCursus_SignalListener(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			cursus.SignalListener()
		})
	}
}

func TestCursus_StartTCP_TLS(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// optional tests here if you please

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			cursus.StartTCP_TLS()
		})
	}
}

func TestCursus_ValidatePermission(t *testing.T) {
	type fields struct {
		TCPAddr         *net.TCPAddr
		TCPListener     *net.TCPListener
		Wg              *sync.WaitGroup
		NodeConnections []*NodeConnection
		SignalChannel   chan os.Signal
		Config          Config
		TLSConfig       *tls.Config
		ContextCancel   context.CancelFunc
		ConfigMu        *sync.RWMutex
		Context         context.Context
		LogMu           *sync.Mutex
		LogFile         *os.File
	}
	type args struct {
		perm string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{want: true, args: args{perm: "R"}},
		{want: true, args: args{perm: "RW"}},
		// optional tests here if you please
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursus := &Cursus{
				TCPAddr:         tt.fields.TCPAddr,
				TCPListener:     tt.fields.TCPListener,
				Wg:              tt.fields.Wg,
				NodeConnections: tt.fields.NodeConnections,
				SignalChannel:   tt.fields.SignalChannel,
				Config:          tt.fields.Config,
				TLSConfig:       tt.fields.TLSConfig,
				ContextCancel:   tt.fields.ContextCancel,
				ConfigMu:        tt.fields.ConfigMu,
				Context:         tt.fields.Context,
				LogMu:           tt.fields.LogMu,
				LogFile:         tt.fields.LogFile,
			}
			if got := cursus.ValidatePermission(tt.args.perm); got != tt.want {
				t.Errorf("ValidatePermission() = %v, want %v", got, tt.want)
			}
		})
	}
}
