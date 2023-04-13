package main

import (
	"errors"
	"fmt"
	"strings"
)

func main() {
	str := "GET /index.html HTTP/1.1"
	res, err := checkFirstLine(str)
	for _, x := range res {
		fmt.Printf("x: %v\n", x)
	}
	fmt.Printf("err: %v\n", err)
}
func checkFirstLine(fline string) ([]string, error) {
	f1 := strings.Split(fline, " ")
	for i := 0; i < len(f1); i++ {
		f1[i] = strings.Replace(f1[i], " ", "", -1)
	}
	if len(f1) == 3 && f1[0] == "GET" && f1[1][0] == '/' && f1[2] == "HTTP/1.1" {
		return f1, nil
	}
	return nil, errors.New("404")
}
