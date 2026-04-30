package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type ValueType string

const (
	TypeString ValueType = "string"
	TypeList   ValueType = "list"
)

type Server struct {
	mu          sync.Mutex
	kv          map[string]ValueEntry
	sset        map[string]map[string]float64
	keyVersions map[string]int64
	waiters     map[string][]chan string
	subscribers map[string]map[*Client]struct{}
}

type Client struct {
	conn          net.Conn
	cmdList       [][]string
	queueCmds     bool
	watchedKeys   map[string]int64
	subscribeMode SubscribeMode
	messages      chan MessageInfo
}

type MessageInfo struct {
	channel string
	message string
}

type SubscribeMode struct {
	enabled  bool
	channels map[string]struct{}
}

type ValueEntry struct {
	valueType ValueType
	strValue  string
	listValue []string
	expiresAt time.Time
	hasExpiry bool
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	server := &Server{
		kv:          make(map[string]ValueEntry),
		sset:        make(map[string]map[string]float64),
		keyVersions: make(map[string]int64),
		waiters:     make(map[string][]chan string),
		subscribers: make(map[string]map[*Client]struct{}),
	}

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go server.handleConn(&Client{
			conn:     conn,
			messages: make(chan MessageInfo),
		})
	}
}

// handleConn handles a connection and responds to the messages being
// written in it.
func (s *Server) handleConn(client *Client) {
	defer client.conn.Close()

	go client.writePubSubMessages()

	for {
		// Read message from the connection and check if its PING.
		data := make([]byte, 1024) // 1 KB buffer.
		n, err := client.conn.Read(data)
		if err != nil {
			fmt.Println("Error reading message from connection: ", err.Error())
			os.Exit(1)
		}

		// Need to parse input and extract arguments from it.
		m := string(data[:n])
		message := strings.Split(m, "\r\n")

		resp, err := s.invokeCmdHandler(client, message)
		if err != nil {
			fmt.Println("Error while invoking handler: ", err.Error())
			os.Exit(1)
		}

		_, err = client.conn.Write([]byte(resp))
		if err != nil {
			fmt.Println("Error writing message into connection: ", err.Error())
			os.Exit(1)
		}
	}
}

// invokeCmdHandler handles invoking the right handler based on
// the command passed by the user.
func (s *Server) invokeCmdHandler(client *Client, message []string) (string, error) {
	resp := ""
	var err error
	cmd := strings.ToUpper(message[2])

	if client.subscribeMode.enabled && !isAllowedInSubscribeMode(cmd) {
		return fmt.Sprintf("-ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context\r\n", cmd), nil
	}

	switch cmd {
	case "ECHO":
		resp, err = handleEchoCmd(message)
		if err != nil {
			return "", fmt.Errorf("error calling ECHO cmd: %w", err)
		}
	case "SET":
		resp, err = s.handleSetCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling SET cmd: %w", err)
		}
	case "GET":
		resp, err = s.handleGetCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling GET cmd: %w", err)
		}
	case "INCR":
		resp, err = s.handleIncrCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling INCR cmd: %w", err)
		}
	case "MULTI":
		resp, err = handleMultiCmd(client)
		if err != nil {
			return "", fmt.Errorf("error calling MULTI cmd: %w", err)
		}
	case "EXEC":
		resp, err = s.handleExecCmd(client)
		if err != nil {
			return "", fmt.Errorf("error calling EXEC cmd: %w", err)
		}
	case "DISCARD":
		resp, err = handleDiscardCmd(client)
		if err != nil {
			return "", fmt.Errorf("error calling DISCARD cmd: %w", err)
		}
	case "RPUSH":
		resp, err = s.handleRPUSHCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling RPUSH cmd: %w", err)
		}
	case "LPUSH":
		resp, err = s.handleLPUSHCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling LPUSH cmd: %w", err)
		}
	case "LRANGE":
		resp, err = s.handleLRANGECmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling RPUSH cmd: %w", err)
		}
	case "LLEN":
		resp, err = s.handleLLENCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling LLEN cmd: %w", err)
		}
	case "LPOP":
		resp, err = s.handleLPOPCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling LPOP cmd: %w", err)
		}
	case "BLPOP":
		resp, err = s.handleBLPOPCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling BLPOP cmd: %w", err)
		}
	case "SUBSCRIBE":
		resp, err = s.handleSubscribeCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling SUBSCRIBE cmd: %w", err)
		}
	case "UNSUBSCRIBE":
		resp, err = s.handleUnsubscribeCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling UNSUBSCRIBE cmd: %w", err)
		}

	case "PUBLISH":
		resp, err = s.handlePublishCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling PUBLISH cmd: %w", err)
		}
	case "ZADD":
		resp, err = s.handleZADDcmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling ZADD cmd: %w", err)
		}
	case "ZRANK":
		resp, err = s.handleZRANKCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling ZRANK cmd: %w", err)
		}
	case "ZRANGE":
		resp, err = s.handleZRANGECmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling ZRANGE cmd: %w", err)
		}
	case "ZCARD":
		resp, err = s.handleZCARDcmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling ZCARD cmd: %w", err)
		}
	case "ZSCORE":
		resp, err = s.handleZSCOREcmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling ZSCORE cmd: %w", err)
		}
	case "ZREM":
		resp, err = s.handleZREMcmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling ZREM cmd: %w", err)
		}
	case "GEOADD":
		resp, err = s.handleGEOADDCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling GEOADD cmd: %w", err)
		}
	case "GEOPOS":
		resp, err = s.handleGEOPOSCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling GEOPOS cmd: %w", err)
		}
	case "GEODIST":
		resp, err = s.handleGEODISTCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling GEODIST cmd: %w", err)
		}
	case "GEOSEARCH":
		resp, err = s.handleGEOSEARCHCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling GEOSEARCH cmd: %w", err)
		}
	case "ACL":
		resp, err = s.handleACLCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling GEOSEARCH cmd: %w", err)
		}
	case "WATCH":
		resp, err = s.handleWATCHCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling GEOSEARCH cmd: %w", err)
		}
	default:
		if client.subscribeMode.enabled {
			return "*2\r\n$4\r\npong\r\n$0\r\n\r\n", nil
		}
		return "+PONG\r\n", nil
	}
	return resp, nil
}
