package tera

import (
	"fmt"
	"tera"
	"testing"
)

func TestTera(*testing.T) {
	fmt.Println("Hello terago!")
	client, c_err := tera.NewClient("./tera.flag", "terago")
	defer client.Close()
	if c_err != nil {
		panic("tera.NewClient error: " + c_err.Error())
	}

	table, t_err := client.OpenTable("terago")
	defer table.Close()
	if t_err != nil {
		panic("tera.OpenTable error: " + t_err.Error())
	}

	p_err := table.PutKV("hello", "terago")
	if p_err != nil {
		panic("put key value error: " + p_err.Error())
	}

	// get an exist key value, return value
	value, g_err := table.GetKV("hello")
	if g_err != nil {
		panic("get key value error: " + g_err.Error())
	}
	fmt.Printf("get key[%s] value[%s].\n", "hello", value)

	// get a not-exist key value, return "not found"
	value, g_err = table.GetKV("hell")
	if g_err == nil {
		panic("get key value should fail: " + g_err.Error())
	}
}
