from nose.tools import *
import subprocess
import time
import os 
    
def print_debug_msg(sid=0, msg=""):
    print "@%d======================%s"%(sid,msg)
    
def setUp():
    print_debug_msg(1, "start master, ts1, ts2, ts3 and status is ok")
    
    cmd = "rm -rf ../log_test/*"
    print cmd
    ret = subprocess.Popen(cmd, shell=True)
    if ret.poll() is not None:
        time.sleep(1)
    
def test_create_table():
    print_debug_msg(2, "create table_test001 and create table_test002(kv), write and check")

    cmd = "cd ../; ./teracli createbyfile testcase/data/create_table_schema; cd testcase/"
    print cmd
    fout = open('../log_test/create_table_test001_out', 'w')
    ferr = open('../log_test/create_table_test001_err', 'w')
    ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)
    if ret.poll() is not None:
        time.sleep(1)
    fout.close()
    ferr.close()
    time.sleep(2)
    assert_true(os.path.getsize("../log_test/create_table_test001_err") == 0)
    
    cmd = 'cd ../; ./teracli create "table_test002 <storage=flash, splitsize=2048, mergesize=128>"; cd testcase/'
    print cmd
    fout = open('../log_test/create_table_test002_out', 'w')
    ferr = open('../log_test/create_table_test002_err', 'w')
    ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)                                           
    if ret.poll() is not None:                                                                                  
        time.sleep(1)                                                                                           
    fout.close()                                                                                                
    ferr.close()                                                                                                
    time.sleep(2)
    assert_true(os.path.getsize("../log_test/create_table_test002_err") == 0)                                   
                                                                                                                
def test_show_table():                                                                                          
    print_debug_msg(3, "show and show(x) table")    

    cmd = "cd ../; ./teracli show; cd testcase/"
    fout = open('../log_test/show_out', 'w')
    ferr = open('../log_test/show_err', 'w')
    ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)
    if ret.poll() is not None:
        time.sleep(1)
    fout.close()
    ferr.close()
    time.sleep(1)
    assert_true(os.path.getsize("../log_test/show_err") == 0)

    cmd = "cd ../; ./teracli showx; cd testcase/"
    print cmd
    fout = open('../log_test/showx_out', 'w')
    ferr = open('../log_test/showx_err', 'w')
    ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)
    if ret.poll() is not None:
        time.sleep(1)
    fout.close()
    ferr.close()
    time.sleep(1)
    assert_true(os.path.getsize("../log_test/showx_err") == 0)

    cmd = "cd ../; ./teracli show table_test001; cd testcase/"
    print cmd
    fout = open('../log_test/show_table_test001_out', 'w')
    ferr = open('../log_test/show_table_test001_err', 'w')
    ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)
    if ret.poll() is not None:
        time.sleep(1)
    fout.close()
    ferr.close()
    time.sleep(1)
    assert_true(os.path.getsize("../log_test/show_table_test001_err") == 0)

    cmd = "cd ../; ./teracli show table_test002; cd testcase/"
    print cmd
    fout = open('../log_test/show_table_test002_out', 'w')                                                      
    ferr = open('../log_test/show_table_test002_err', 'w')                                                      
    ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)                                           
    if ret.poll() is not None:
        time.sleep(1)
    fout.close()                                                                                                
    ferr.close()                                                                                                
    time.sleep(1)
    assert_true(os.path.getsize("../log_test/show_table_test002_err") == 0) 

    cmd = "cd ../; ./teracli showx table_test001; cd testcase/"                                                 
    print cmd                                                                                                   
    fout = open('../log_test/showx_table_test001_out', 'w')                                                     
    ferr = open('../log_test/showx_table_test001_err', 'w')                                                     
    ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)                                           
    if ret.poll() is not None:
        time.sleep(1)
    fout.close()                                                                                                
    ferr.close()                                                                                                
    time.sleep(1)
    assert_true(os.path.getsize("../log_test/showx_table_test001_err") == 0)                                    
                                                                                                                
    cmd = "cd ../; ./teracli showx table_test002; cd testcase/"                                                 
    print cmd                                                                                                   
    fout = open('../log_test/showx_table_test002_out', 'w')                                                     
    ferr = open('../log_test/showx_table_test002_err', 'w')                                                     
    ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)                                           
    if ret.poll() is not None:
        time.sleep(1)
    fout.close()                                                                                                
    ferr.close()                                                                                                
    time.sleep(1)
    assert_true(os.path.getsize("../log_test/showx_table_test002_err") == 0)                                    
                                                                                                                
    cmd = "cd ../; ./teracli showschema table_test001; cd testcase/"                                            
    print cmd                                                                                                   
    fout = open('../log_test/showschema_table_test001_out', 'w')                                                
    ferr = open('../log_test/showschema_table_test001_err', 'w')                                                
    ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)                                           
    if ret.poll() is not None:
        time.sleep(1)
    fout.close()                                                                                                
    ferr.close()                                                                                                
    time.sleep(1)
    assert_true(os.path.getsize("../log_test/showschema_table_test001_err") == 0)

    cmd = "cd ../; ./teracli showschema table_test002; cd testcase/"                                            
    print cmd                                                                                                   
    fout = open('../log_test/showschema_table_test002_out', 'w')                                                
    ferr = open('../log_test/showschema_table_test002_err', 'w')                                                
    ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)                                           
    if ret.poll() is not None:
        time.sleep(1)
    fout.close()                                                                                                
    ferr.close()                                                                                                
    time.sleep(1)
    assert_true(os.path.getsize("../log_test/showschema_table_test002_err") == 0) 

    cmd = "cd ../; ./teracli showschemax table_test001; cd testcase/"                                           
    print cmd                                                                                                   
    fout = open('../log_test/showschemax_table_test001_out', 'w')                                               
    ferr = open('../log_test/showschemax_table_test001_err', 'w')                                               
    ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)                                           
    if ret.poll() is not None:
        time.sleep(1)
    fout.close()                                                                                                
    ferr.close()                                                                                                
    time.sleep(1)
    assert_true(os.path.getsize("../log_test/showschemax_table_test001_err") == 0)

    cmd = "cd ../; ./teracli showschemax table_test002; cd testcase/"                                           
    print cmd                                                                                                   
    fout = open('../log_test/showschemax_table_test002_out', 'w')                                               
    ferr = open('../log_test/showschemax_table_test002_err', 'w')                                               
    ret = subprocess.Popen(cmd, stdout=fout, stderr=ferr, shell=True)                                           
    if ret.poll() is not None:
        time.sleep(1)
    fout.close()                                                                                                
    ferr.close()                                                                                                
    time.sleep(1)
    assert_true(os.path.getsize("../log_test/showschemax_table_test002_err") == 0)

def tearDown():
    print_debug_msg(4, "delete table_test001 and table_test002, clear env")

    cmd = "cd ../; ./teracli disable table_test001; cd testcase/"
    print cmd
    ret = subprocess.Popen(cmd, shell=True)
    time.sleep(2)

    cmd = "cd ../; ./teracli drop table_test001; cd testcase/"
    print cmd
    ret = subprocess.Popen(cmd, shell=True)
    time.sleep(2)

    cmd = "cd ../; ./teracli disable table_test002; cd testcase/"
    print cmd
    ret = subprocess.Popen(cmd, shell=True)
    time.sleep(2)

    cmd = "cd ../; ./teracli drop table_test002; cd testcase/"
    print cmd
    ret = subprocess.Popen(cmd, shell=True)
    time.sleep(2)

