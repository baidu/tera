package tera

/*
#cgo LDFLAGS: -ltera_c
#include <stdio.h>
#include <stdlib.h>
#include <tera_c.h>
tera_client_t* client_open(const char* conf_path, const char* log_prefix) {
	char *err = NULL;
	tera_client_t* cli = tera_client_open(conf_path, log_prefix, &err);
	if (err != NULL) {
		fprintf(stderr, "tera client open error: %s.\n", err);
		free(err);
	}
	return cli;
}

tera_table_t* table_open(tera_client_t* client, const char* table_name) {
	char *err = NULL;
	tera_table_t* table = tera_table_open(client, table_name, &err);
	if (err != NULL) {
		fprintf(stderr, "tera table open error: %s.\n", err);
		free(err);
	}
	return table;
}
*/
import "C"
import (
	"errors"
	"fmt"
)

type Client struct {
	ConfPath string
	CClient  *C.tera_client_t
}

func NewClient(conf_path, log_prefix string) (client Client, err error) {
	fmt.Println("new client")
	client.CClient = C.client_open(C.CString(conf_path), C.CString(log_prefix))
	client.ConfPath = conf_path
	if client.CClient == nil {
		err = errors.New("Fail to create tera client")
	}
	return
}

func (c *Client) Close() {
	fmt.Println("close client")
	if c.CClient != nil {
		C.tera_client_close(c.CClient)
	}
}

func (c *Client) OpenTable(table_name string) (table Table, err error) {
	fmt.Println("open table: " + table_name)
	if c.CClient == nil {
		err = errors.New("Fail to open table, client not available.")
		return
	}
	c_table := C.table_open(c.CClient, C.CString(table_name))
	table = Table{Name: table_name, CTable: c_table}
	return
}
