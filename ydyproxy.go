/**
change log 2014-08-10 16:24:00

*/
package main

import (
    "fmt"
    "net"
    "runtime"
    "bufio"
    "strings"
    "crypto/md5"
    "time"
    "sync"
    "flag"
    "os"
    "errors"
    "io"
    "io/ioutil"
)


var version = `
ydyproxy 1.0 is a memcahced proxy, Copyright (c) 2014 Qiwen<34214399@qq.com>
All rights reserved. GPL-3.0
`

var lock sync.Mutex

const(
    TIMEOUT = time.Millisecond*20
)

type Driver net.Conn

var master_ip_map []string
var backup_ip_map []string


type Item struct{
    Cmd string
    Key string
    Flags uint32
    Exptime uint32
    Bytes uint32
}

var(
    crlf = []byte("\r\n")
    space = []byte(" ")

    patterns = map[string]string{
        "add":"add %s %d %d %d",
        "set":"set %s %d %d %d",
        "replace":"replace %s %d %d %d",
        "get":"get %s",
        "delete":"delete %s",
        "quit":"quit",
        "VALUE":"VALUE %s %d %d",
    }

    vv = new(bool)
)


func main(){

    fmt.Println( "cpus =", runtime.NumCPU() )

    _p := flag.String("p","11221","port, default is 11211. (0 to disable tcp support)")
    _s := flag.String("s","","ip:port,ip:port... (master group)")
    _b := flag.String("b","","ip:port,ip:port... (backup group)")
    _c := flag.Int("c",runtime.NumCPU()/2, "The total number of threads; default numcpu/2")
    _v := flag.Bool("v",false,"verbose")
    _h := flag.Bool("h",false,"help")
    flag.Parse()

    *vv = *_v

    if *_h == true || *_s=="" {
        flag.Usage()
        fmt.Println( version )
        os.Exit(0)
    }
    if *_c>runtime.NumCPU(){
        *_c=runtime.NumCPU()
    }else if *_c<1 {
        *_c=1
    }
    runtime.GOMAXPROCS( *_c )

    //fmt.Println( *_p, *_s, *_b, *_v, *_h, *_c )

    master_ip_map = strings.Split(*_s,",")
    backup_ip_map = strings.Split(*_b,",")

    defer fmt.Println("ydyproxy exit")

    if l, err := net.Listen("tcp", ":"+*_p); err==nil{
        for{
            if c,err := l.Accept(); err==nil{
                lock.Lock()
                go process(c)
            }
        }
    }else{
        fmt.Println(err)
    }
}

func CheckErr(tag string, err error)(error){

    if err==nil{
        return nil
    }else{
        if *vv==true {
            fmt.Println("tag:"+tag, err)
        }
        return err
    }
}

func ConnectMecachedConn(driver_map *[]Driver,ips []string){
    for _,ip := range ips{
        if _m, err := net.DialTimeout("tcp", ip, TIMEOUT); err==nil{
            *driver_map = append(*driver_map, _m) 
        }else{
            *driver_map = append(*driver_map, nil) 
        }
    }
}

func DisconnectMecachedConn(driver_map *[]Driver){
    for _,driver := range *driver_map{
        if driver!=nil{
            driver.Close()
        }
    }
}

func Write(c Driver, data... []byte)(err error){

    w := bufio.NewWriter(c)

    for _, _byte := range data{
        _, err = w.Write( _byte )
        if CheckErr("memcahced write data", err)!=nil{
            return err
        }
    }   

    err = w.Flush()
    if CheckErr("memcahced flush ", err)!=nil{
        return err
    }

    return nil
}

func ReadCallBack(c Driver)([]byte, error){
    cr := bufio.NewReader(c)

    command,_,err := cr.ReadLine()
    if err!=nil{
        return nil,err
    }

    line := fmt.Sprintf("%s", command)

    //
    item := new(Item)
    _, err = scanGetResponseLine(line, item)


    //
    data_body := []byte(line+"\r\n")
    if err!=nil{
        return data_body, nil
    }

    //
    read_data, err := ioutil.ReadAll(io.LimitReader(cr, int64(item.Bytes)+7))
    if err!=nil{
        return nil, err
    }

    for _,b := range read_data {
        data_body = append(data_body, b)
    }

    return data_body, nil
}


func Getmd5(str string,ips []string) int{
    h := md5.New()
    h.Write([]byte(str)) // 需要加密的字符串为 123456

    md5int := h.Sum(nil)

    return int(md5int[0])%len(ips)
}


func scanGetResponseLine(line string, it *Item)(string, error){

    command := strings.Fields(line)

//    dest := []interface{}{ &it.Key, &it.Flags, &it.Exptime, &it.Bytes }
//    if len(command)==0{
//        dest = []interface{}{}
//    }else{
//        dest = dest[:len(command)-1]
//    } 


    dest := []interface{}{}
    switch len(command){
    case 2:
        dest = []interface{}{ &it.Key }
    case 4:
        dest = []interface{}{ &it.Key, &it.Flags, &it.Bytes }
    case 5:
        dest = []interface{}{ &it.Key, &it.Flags, &it.Exptime, &it.Bytes }
    }


    for k,v := range patterns{
        _, err := fmt.Sscanf( line, v, dest...  )
        if err==nil {
            return k, nil
        }
    }

    return "",errors.New("not command") 
}


func process(c net.Conn){

    var master_driver_map []Driver
    ConnectMecachedConn(&master_driver_map, master_ip_map)

    var backup_driver_map []Driver
    ConnectMecachedConn(&backup_driver_map, backup_ip_map)

    sr := bufio.NewReader(c)
    for{
        command,_,err := sr.ReadLine()
        if CheckErr("request read line", err)!=nil {
            break
        }
        command_string := fmt.Sprintf("%s", command)

//        fmt.Println( command_string )
        //
        item := new(Item)
        command_name, err := scanGetResponseLine(command_string, item)
        if CheckErr("command err", err)!=nil{
            continue
        }

        //count
        master_index := Getmd5(item.Key, master_ip_map)
        backup_index := Getmd5(item.Key, backup_ip_map)

        master_client := master_driver_map[master_index]
        backup_client := backup_driver_map[backup_index]

        //
        var data_body []byte
        switch command_name {
        case "set","add","replace":
        //,"append","prepend","cas":
            read_data, err := ioutil.ReadAll(io.LimitReader(sr, int64(item.Bytes+2) ))
            if err!=nil{
                continue
            }
            data_body = read_data
            //
            if master_client!=nil{
                CheckErr("request master ", errors.New(master_ip_map[master_index]+"\t"+command_string ) )
                CheckErr("\trquest backup ", errors.New(backup_ip_map[backup_index]+"\t"+command_string ) )
                Write(backup_client, command, crlf, data_body ) 
            }
        default:
            if master_client==nil{
                //if master invalid; read backup
                CheckErr("only read;change backup ", errors.New(backup_ip_map[backup_index]+"\t"+command_string ) )
                master_client = backup_client
            }else{
                CheckErr("request master ", errors.New(master_ip_map[master_index]+"\t"+command_string ) )
            }
        }

        //if backupup don't connected
        if master_client==nil{
            break
        }
        
        if Write(master_client, command, crlf, data_body )!=nil{
            continue
        }  

        //request memcahced callback data
        callback_data, err := ReadCallBack(master_client)
        if( err!=nil ){
            continue
        }

        if Write(c, callback_data)!=nil{
            continue
        }

    }

    DisconnectMecachedConn(&master_driver_map)
    DisconnectMecachedConn(&backup_driver_map)

    c.Close()

    lock.Unlock()
}

