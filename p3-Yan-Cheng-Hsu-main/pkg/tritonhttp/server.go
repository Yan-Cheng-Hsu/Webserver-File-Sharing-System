package tritonhttp

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

type Server struct {
	// Addr specifies the TCP address for the server to listen on,
	// in the form "host:port". It shall be passed to net.Listen()
	// during ListenAndServe().
	Addr string // e.g. ":0"
	// DocRoot specifies the path to the directory to serve static files from.
	DocRoot string
}

//declaration of clients' map
//var clients = make(map[string]net.Conn)

//check if file path exists

// ListenAndServe listens on the TCP network address s.Addr and then
// handles requests on incoming connections.
func (s *Server) ListenAndServe() error {
	//panic("todo")
	// Hint: call HandleConnection
	listener, _ := net.Listen("tcp", s.Addr)
	/* 	if err != nil {
		log.Fatal(err)
	} */
	for {
		conn, _ := listener.Accept()
		fmt.Printf("Recieved a new connection %v.\n", conn.RemoteAddr())
		go s.HandleConnection(conn)
	}

}

// HandleConnection reads requests from the accepted conn and handles them.
func (s *Server) HandleConnection(conn net.Conn) {
	br := bufio.NewReader(conn)
	for {
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		req, bytesReceived, err := ReadRequest(br)
		fmt.Printf("bytesReceived: %v\n", bytesReceived)
		if err != nil {
			if bytesReceived {
				res := &Response{}
				res.HandleBadRequest()
				res.Write(conn)
			}
			conn.Close()
			break
		}
		res := s.HandleGoodRequest(req)
		res.Write(conn)
		if req.Close {
			conn.Close()
			break
		}
	}
}

func is200(req *Request, rootPath string) bool {
	req.URL = rootPath + req.URL
	if req.URL[len(req.URL)-1] == '/' {
		req.URL += "index.html"
	}
	fmt.Printf("req.URL: %v\n", req.URL)
	_, err := os.Stat(req.URL)
	return err == nil
}

// HandleGoodRequest handles the valid req and generates the corresponding res.
func (s *Server) HandleGoodRequest(req *Request) (res *Response) {
	//panic("todo")
	// Hint: use the other methods below
	isOk := is200(req, s.DocRoot)
	res = &Response{}
	if !isOk {
		res.HandleNotFound(req)
	} else {
		res.HandleOK(req, s.DocRoot)
	}
	return res
}

// HandleOK prepares res to be a 200 OK response
// ready to be written back to client.
func (res *Response) HandleOK(req *Request, path string) {
	res.StatusCode = 200
	res.Proto = "HTTP/1.1"
	res.FilePath = req.URL
	res.Header = make(map[string]string)
	if req.Close {
		res.Header["Connection"] = "close"
	}
	res.Header["Last-Modified"] = FormatTime(time.Now())
	//deal with MIME Content-Type
	idxOfDot := len(req.URL) - 1
	for ; idxOfDot >= 0 && req.URL[idxOfDot] != '.'; idxOfDot-- {
	}
	res.Header["Content-Type"] = MIMETypeByExtension(req.URL[idxOfDot:])
	res.Header["Date"] = FormatTime(time.Now())
	fileInfo, _ := os.Stat(res.FilePath)
	fmt.Printf("res.FilePath: %v\n", res.FilePath)
	res.Header["Content-Length"] = strconv.FormatInt(fileInfo.Size(), 10)

}

// HandleBadRequest prepares res to be a 400 Bad Request response
// ready to be written back to client.
func (res *Response) HandleBadRequest() {
	res.StatusCode = 400
	res.Proto = "HTTP/1.1"
	res.Header = make(map[string]string)
	res.Header["Connection"] = "close"
	res.Header["Date"] = FormatTime(time.Now())
}

// HandleNotFound prepares res to be a 404 Not Found response
// ready to be written back to client.
func (res *Response) HandleNotFound(req *Request) {
	res.StatusCode = 404
	res.Proto = "HTTP/1.1"
	res.Request = req
	res.Header = make(map[string]string)
	res.Header["Date"] = FormatTime(time.Now())
	if req.Close {
		res.Header["Connection"] = "close"
	}

}
