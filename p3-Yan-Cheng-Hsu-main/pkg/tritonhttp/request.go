package tritonhttp

import (
	"bufio"
	"errors"
	"strings"
)

type Request struct {
	Method string // e.g. "GET"
	URL    string // e.g. "/path/to/a/file"
	Proto  string // e.g. "HTTP/1.1"

	// Header stores misc headers excluding "Host" and "Connection",
	// which are stored in special fields below.
	// Header keys are case-incensitive, and should be stored
	// in the canonical format in this map.
	Header map[string]string

	Host  string // determine from the "Host" header
	Close bool   // determine from the "Connection" header
}

// ReadRequest tries to read the next valid request from br.
//
// If it succeeds, it returns the valid request read. In this case,
// bytesReceived should be true, and err should be nil.
//
// If an error occurs during the reading, it returns the error,
// and a nil request. In this case, bytesReceived indicates whether or not
// some bytes are received before the error occurs. This is useful to determine
// the timeout with partial request received condition.
func ReadRequest(br *bufio.Reader) (req *Request, bytesReceived bool, err error) {
	// initialize
	_, e := br.Peek(1)
	if e != nil {
		return nil, false, e
	}
	req = &Request{}
	req.Header = make(map[string]string)
	// parse start line
	var fline string
	fline, err = ReadLine(br)
	if err != nil {
		return nil, true, err
	}
	f1, err := checkFirstLine(fline)
	if err != nil {
		return nil, true, err
	}
	req.Method = f1[0]
	req.URL = f1[1]
	req.Proto = f1[2]

	// parse headers
	for {
		line, err := ReadLine(br)
		if len(line) == 0 {
			break
		}
		if err != nil {
			return nil, true, err
		}
		key, val, err := checkHeader(line)
		if err != nil {
			return nil, true, err
		}
		req.Header[key] = val
	}

	// Check required headers
	var ok bool
	req.Host, ok = req.Header["Host"]
	if !ok {
		return nil, true, errors.New("400")
	}
	delete(req.Header, "Host")

	connection := false
	var c string
	c, ok = req.Header["Connection"]
	if ok && c == "close" {
		connection = true
		delete(req.Header, "Connection")
	} else if ok && c != "close" {
		delete(req.Header, "Connection")
	}
	req.Close = connection
	return req, true, nil
}

func checkFirstLine(fline string) ([]string, error) {
	f1 := strings.Split(fline, " ")
	for i := 0; i < len(f1); i++ {
		f1[i] = strings.Replace(f1[i], " ", "", -1)
	}
	if len(f1) == 3 && f1[0] == "GET" && f1[1][0] == '/' && f1[2] == "HTTP/1.1" {
		return f1, nil
	}
	return nil, errors.New("400")
}
func checkHeader(header string) (string, string, error) {
	idxofColon := 0
	for ; idxofColon < len(header) && header[idxofColon] != ':'; idxofColon++ {
	}
	idxofSpace := 0
	for ; idxofSpace < len(header) && header[idxofSpace] != ' '; idxofSpace++ {
	}

	if idxofColon == 0 || idxofSpace < idxofColon {
		return "", "", errors.New("400")
	}
	temp1 := header[:idxofColon]
	var temp2 string
	if idxofColon+1 < len(header) {
		temp2 = header[idxofColon+1:]
	} else {
		temp2 = ""
	}
	key := strings.Replace(temp1, " ", "", -1)
	val := strings.Replace(temp2, " ", "", -1)
	if len(key) == 0 {
		return "", "", errors.New("400")
	}

	return CanonicalHeaderKey(key), val, nil

}
