package tritonhttp

import (
	"errors"
	"io"
	"log"
	"os"
)

/* func String2Bytes(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}
	return *(*[]byte)(unsafe.Pointer(&bh))
} */

type Response struct {
	StatusCode int    // e.g. 200
	Proto      string // e.g. "HTTP/1.1"
	// Header stores all headers to write to the response.
	// Header keys are case-incensitive, and should be stored
	// in the canonical format in this map.
	Header map[string]string

	// Request is the valid request that leads to this response.
	// It could be nil for responses not resulting from a valid request.
	Request *Request

	// FilePath is the local path to the file to serve.
	// It could be "", which means there is no file to serve.
	FilePath string
}

// Write writes the res to the w.
func (res *Response) Write(w io.Writer) error {
	if err := res.WriteStatusLine(w); err != nil {
		return err
	}
	if err := res.WriteSortedHeaders(w); err != nil {
		return err
	}
	if err := res.WriteBody(w); err != nil {
		return err
	}
	return nil
}

// WriteStatusLine writes the status line of res to w, including the ending "\r\n".
// For example, it could write "HTTP/1.1 200 OK\r\n".
func (res *Response) WriteStatusLine(w io.Writer) error {
	//panic("todo")
	fline := res.Proto
	if res.StatusCode == 200 {
		fline = fline + " 200 OK\r\n"
	} else if res.StatusCode == 400 {
		fline = fline + " 400 Bad Request\r\n"
	} else {
		fline = fline + " 404 Not Found\r\n"
	}
	//out := String2Bytes(fline)
	//fmt.Println("1st line: ", fline)
	w.Write([]byte(fline))
	return nil
}

// WriteSortedHeaders writes the headers of res to w, including the ending "\r\n".
// For example, it could write "Connection: close\r\nDate: foobar\r\n\r\n".
// For HTTP, there is no need to write headers in any particular order.
// TritonHTTP requires to write in sorted order for the ease of testing.
func (res *Response) WriteSortedHeaders(w io.Writer) error {
	//panic("todo")
	//for code 200 --> Content-Length
	var a [][]byte
	key_map := a
	for key, _ := range res.Header {
		key_map = append(key_map, []byte(key))
	}
	qsort(key_map, 0, len(key_map)-1)

	out := ""
	for _, key := range key_map {
		val := res.Header[string(key)]
		out = out + string(key) + ": " + val + "\r\n"
	}
	out = out + "\r\n"
	//fmt.Println(out)
	w.Write([]byte(out))
	return nil
}

// WriteBody writes res' file content as the response body to w.
// It doesn't write anything if there is no file to serve.
func (res *Response) WriteBody(w io.Writer) error {
	//panic("todo")
	// when filepath == "" --> no body to write
	rferr := errors.New("read body file error")
	if res.FilePath == "" {
		return nil
	} else {
		f, err := os.ReadFile(res.FilePath)
		if err != nil {
			err = rferr
			log.Println(err)
			return err
		}
		w.Write(f)
		return nil
	}
}

func Bigger(a []byte, b []byte) bool {
	i := 0
	for i < len(a) && i < len(b) {
		if int(a[i]) > int(b[i]) {
			return true
		} else if int(a[i]) < int(b[i]) {
			return false
		} else {
			i += 1
		}
	}

	if len(a) > len(b) {
		return true
	} else {
		return false
	}
}
func qsort(nums [][]byte, start int, end int) {
	if end <= start {
		return
	}

	pivot := nums[start]

	l := start + 1
	r := end

	for {
		for r > start && Bigger(nums[r], pivot) {
			r -= 1
		}

		for l <= r && !Bigger(nums[l], pivot) {
			l += 1
		}

		if l < r {
			nums[l], nums[r] = nums[r], nums[l]
		} else {
			if r > start {
				nums[start], nums[r] = nums[r], nums[start]
			}

			break
		}
	}

	if r > start {
		qsort(nums, start, r-1)
	}

	if r < end {
		qsort(nums, r+1, end)
	}
}
