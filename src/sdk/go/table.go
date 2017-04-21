package tera

/*
#cgo LDFLAGS: -ltera_c
#include <stdio.h>
#include <stdlib.h>
#include <tera_c.h>
bool table_put_kv_sync(tera_table_t* table,
                       const char* key, unsigned long long keylen,
                       const char* value, unsigned long long vallen) {
	char* err = NULL;
	bool ret = tera_table_put(table, key, keylen, "", "", 0, value, vallen, &err);
	if (!ret) {
		fprintf(stderr, "tera put kv error: %s.\n", err);
		free(err);
	}
	return ret;
}

char* table_get_kv_sync(tera_table_t* table, const char* key, int keylen, int* vallen) {
	uint64_t vlen = 0;
	char* err = NULL;
	char* value;
	bool ret = tera_table_get(table, key, keylen, "", "", 0, &value, &vlen, &err, 0);
	if (ret) {
	  *vallen = (int)vlen;
  } else {
		*vallen = -1;
		fprintf(stderr, "tera get kv error: %s.\n", err);
		free(err);
	}
  return value;
}
*/
import "C"
import (
	"errors"
	"fmt"
	"unsafe"
)

type Table struct {
	Name   string
	CTable *C.tera_table_t
}

func (t *Table) Close() {
	fmt.Println("close table: " + t.Name)
	if t.CTable != nil {
		C.tera_table_close(t.CTable)
	}
}

func (t *Table) PutKV(key, value string) (err error) {
	if t.CTable == nil {
		return errors.New("table not open: " + t.Name)
	}
	ret := C.table_put_kv_sync(t.CTable, C.CString(key), C.ulonglong(len(key)),
		C.CString(value), C.ulonglong(len(value)))
	if !ret {
		err = errors.New("put kv error")
	}
	return
}

func (t *Table) GetKV(key string) (value string, err error) {
	if t.CTable == nil {
		err = errors.New("table not open: " + t.Name)
		return
	}
	var vallen C.int
	vc := C.table_get_kv_sync(t.CTable, C.CString(key), C.int(len(key)), (*C.int)(&vallen))
	if vallen >= 0 {
		value = C.GoStringN(vc, vallen)
		C.free(unsafe.Pointer(vc))
	} else {
		err = errors.New("key not found")
		value = ""
	}
	return
}
