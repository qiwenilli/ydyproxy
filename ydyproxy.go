/**
change log 1.0 2014-08-10 16:24
change log 1.1 2014-08-27 16:40
change log 1.2 2014-09-12 12:58
change log 1.3 2014-09-17 17:39 add "gets" command support
*/

package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	//    "time"
	//    "crypto/md5"
)

var lock = &sync.Mutex{}

var (
	version = `
ydyproxy 1.3 is a memcahced proxy, Copyright (c) 2014 Qiwen<34214399@qq.com>
All rights reserved. GPL-3.0
`
	master_ip_map []string //{"127.0.0.1:11221","127.0.0.1:11222"}
	backup_ip_map []string //{"127.0.0.1:11223"}

	crlf                  = []byte("\r\n")
	end                   = []byte("END\r\n")
	memcached_error       = []byte("ERROR\r\n")

	vv = new(bool)

	ERR_NOTCONNECT = errors.New("memcahced invalid")
	ERR_COMMAND    = errors.New("command invalid")
)

type Driver struct {
	Conn net.Conn
	W    *bufio.Writer
	R    *bufio.Reader
}

func main() {

	fmt.Println("cpus =", runtime.NumCPU())

	_p := flag.String("p", "11221", "port, default is 11211. (0 to disable tcp support)")
	_s := flag.String("s", "", "ip:port,ip:port... (master group)")
	_b := flag.String("b", "", "ip:port,ip:port... (backup group)")
	_c := flag.Int("c", runtime.NumCPU()/2, "The total number of threads; default numcpu/2")
	_d := flag.Bool("D", false, "don't go to background")
	_v := flag.Bool("v", false, "verbose")
	_h := flag.Bool("h", false, "help")
	flag.Parse()

	*vv = *_v

	if *_h == true || *_s == "" {
		flag.Usage()
		fmt.Println(version)
		os.Exit(0)
	}
	if *_c > runtime.NumCPU() {
		*_c = runtime.NumCPU()
	} else if *_c < 1 {
		*_c = 1
	}
	runtime.GOMAXPROCS(*_c)

	//fmt.Println( *_p, *_s, *_b, *_v, *_h, *_c )

	master_ip_map = strings.Split(*_s, ",")
	backup_ip_map = strings.Split(*_b, ",")

	defer fmt.Println("ydyproxy exit")

	if *_d == true && daemon(0, 0) == -1 {
		fmt.Println("failed to be a daemon")
		os.Exit(0)
	}

	//
	l, err := net.Listen("tcp", ":"+*_p)
	if err != nil {
		fmt.Printf("Failure to listen: %s\n", err.Error())
		panic(err)
	}

	for {

		if c, err := l.Accept(); err == nil {
			go process(c)
		} else {
			fmt.Println("accept error", err)
		}
	}
}

func process(c net.Conn) {

	//
	s := &Driver{Conn: c, W: bufio.NewWriter(c), R: bufio.NewReader(c)}
	defer c.Close()

	master_client := connect_memcached(master_ip_map)
	backup_client := connect_memcached(backup_ip_map)

	for {
		line, err := s.R.ReadSlice('\n')
		if err != nil {
			goto END
		}

		cmd_string := string(line[0 : len(line)-2])
		debug(fmt.Sprintf("\t|||source--> %s } ", cmd_string), nil)

		//
		cmd_list, request_data, err := get_request_body(s, line)
		if err != nil {
			debug("ERR>"+fmt.Sprintf("%q", err), nil)
			goto END
		}

		if cmd_list[0] == "gets" {
			var response_data []byte
			for i := 1; i < len(cmd_list); i++ {
				get := fmt.Sprintf("get %s \r\n", cmd_list[i])
				get_byte := []byte(get)
				get_list := strings.Fields(get)
				d, err := driver_memcached(master_client, backup_client, &get_byte, &get_list)

				if err == nil {
					for _, b := range d[0 : len(d)-5] {
						response_data = append(response_data, b)
					}
				} else {
					goto END
				}
			}
			response_data = append(response_data, 69, 78, 68, 13, 10)
			if _, err := s.Conn.Write(response_data); err != nil {
				goto END
			}
		} else {
			d, err := driver_memcached(master_client, backup_client, &request_data, &cmd_list)
			if err == nil {
				if _, err := s.Conn.Write(d); err != nil {
					goto END
				}
			} else {
				goto END
			}
		}
	}

	disconnect_memcached(master_client)
	disconnect_memcached(backup_client)

	return
END:
	s.Conn.Write(memcached_error)

	//
	disconnect_memcached(master_client)
	disconnect_memcached(backup_client)
}

func connect_memcached(ips []string) []*Driver {
	var connect_server []*Driver
	for _, ip := range ips {
		if c, err := net.Dial("tcp", ip); err == nil {
			d := &Driver{Conn: c, W: bufio.NewWriter(c), R: bufio.NewReader(c)}
			connect_server = append(connect_server, d)
		} else {
			connect_server = append(connect_server, nil)
			debug("connect>"+ip+" "+fmt.Sprintf("%s", err), nil)
		}
	}
	return connect_server
}

func disconnect_memcached(drivers []*Driver) {
	for _, d := range drivers {
		if d != nil {
			d.Conn.Close()
		}
	}
}

func get_request_body(s *Driver, cmd []byte) ([]string, []byte, error) {

	cmd_string := fmt.Sprintf("%s", cmd)

	cmd_list := strings.Fields(cmd_string)

	//check command support
	if cmd_list[0] != "get" && cmd_list[0] != "gets" && cmd_list[0] != "add" && cmd_list[0] != "set" && cmd_list[0] != "delete" && cmd_list[0] != "replace" {
		return nil, nil, errors.New(cmd_string + " nonsupport")
	}

	//
	if len(cmd_list) > 1 {
		var request_body []byte
		if cmd_list[0] != "get" && cmd_list[0] != "gets" && len(cmd_list) > 3 {
			request_len, err := strconv.Atoi(cmd_list[4])
			if err != nil {
				return nil, nil, err
			}
			_request_body, err := ioutil.ReadAll(io.LimitReader(s.R, int64(request_len+2)))
			if err != nil {
				return nil, nil, err
			}
			request_body = _request_body
		}
		request_data := make([]byte, len(cmd)+len(request_body))
		copy(request_data, []byte(cmd_string))
		copy(request_data[len(cmd):], request_body)
		//
		return cmd_list, request_data, nil
	}
	return cmd_list, cmd, ERR_COMMAND
}

func get_response_body(m *Driver, cmd []byte) ([]byte, error) {

	cmd_string := fmt.Sprintf("%s", cmd)

	cmd_list := strings.Fields(cmd_string)

	//
	if len(cmd_list) > 1 && cmd_list[0] == "VALUE" {
		response_len, _ := strconv.Atoi(cmd_list[3])
		response_body, err := ioutil.ReadAll(io.LimitReader(m.R, int64(response_len+7)))
		//
		if err != nil {
			return nil, err
		}
		response_data := make([]byte, len(cmd)+len(response_body))
		copy(response_data, []byte(cmd_string))
		copy(response_data[len(cmd):], response_body)
		//
		return response_data, nil
	}
	return cmd, nil
}

func driver_memcached(master_client, backup_client []*Driver, request_data *[]byte, cmd_list *[]string) ([]byte, error) {

	master_index, backup_index := getindex(&((*cmd_list)[1]), len(master_ip_map)), getindex(&(*cmd_list)[1], len(backup_ip_map))
	master_ip, backup_ip := master_ip_map[master_index], backup_ip_map[backup_index]
	master_driver, backup_driver := master_client[master_index], backup_client[backup_index]

	cmd_string := strings.Join(*cmd_list, " ")

	//write master
	response_data, err := write_memcached(master_driver, request_data)
	if err == nil {
		debug("M>"+master_ip+" "+cmd_string, nil)
		//
		if (*cmd_list)[0] != "get" {
			debug("B>"+backup_ip+" "+cmd_string, nil)
			write_memcached(backup_driver, request_data)
		}
		//
		return response_data, err
	} else if (*cmd_list)[0] == "get" {
		//write backup
		debug("B>"+backup_ip+" "+cmd_string, nil)
		response_data, err := write_memcached(backup_driver, request_data)
		if err == nil {
			return response_data, err
		}
	}
	return nil, ERR_COMMAND
}

func write_memcached(m *Driver, request_data *[]byte) ([]byte, error) {

	if m == nil {
		return nil, ERR_NOTCONNECT
	}
	if _, err := m.W.Write(*request_data); err != nil {
		return nil, err
	}
	if err := m.W.Flush(); err != nil {
		return nil, err
	}

	response_command, err := m.R.ReadSlice('\n')
	if err != nil {
		return nil, err
	}

	return get_response_body(m, response_command)
}

func getindex(str *string, num int) int {

	return hashme(str) % num

	//
	/*
	   h := md5.New()
	   h.Write([]byte(str)) // 需要加密的字符串为 123456

	   md5int := h.Sum(nil)

	   return int(md5int[0]+md5int[1]+md5int[2])%num
	*/
}

func hashme(str *string) int {

	if str == nil {
		return 0
	}

	hash := 5381
	for _, b := range *str {
		hash = ((hash << 5) + hash) + int(byte(b))

	}
	hash &= 0x7FFFFFFF
	return hash
}

func daemon(nochdir, noclose int) int {
	var ret, ret2 uintptr
	var err syscall.Errno

	darwin := runtime.GOOS == "darwin"

	// already a daemon
	if syscall.Getppid() == 1 {
		return 0
	}

	// fork off the parent process
	ret, ret2, err = syscall.RawSyscall(syscall.SYS_FORK, 0, 0, 0)
	if err != 0 {
		return -1
	}

	// failure
	if ret2 < 0 {
		os.Exit(-1)
	}

	// handle exception for darwin
	if darwin && ret2 == 1 {
		ret = 0
	}

	// if we got a good PID, then we call exit the parent process.
	if ret > 0 {
		os.Exit(0)
	}

	/* Change the file mode mask */
	_ = syscall.Umask(0)

	// create a new SID for the child process
	s_ret, s_errno := syscall.Setsid()
	if s_errno != nil {
		fmt.Printf("Error: syscall.Setsid errno: %d", s_errno)

	}
	if s_ret < 0 {
		return -1
	}

	if nochdir == 0 {
		os.Chdir("/")
	}

	if noclose == 0 {
		f, e := os.OpenFile("/dev/null", os.O_RDWR, 0)
		if e == nil {
			fd := f.Fd()
			syscall.Dup2(int(fd), int(os.Stdin.Fd()))
			syscall.Dup2(int(fd), int(os.Stdout.Fd()))
			syscall.Dup2(int(fd), int(os.Stderr.Fd()))

		}

	}
	return 0
}

func debug(msg string, err error) {
	if *vv == true {
		fmt.Println(msg, err)
	}
}
