"""
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""

import subprocess
import filecmp
import os
import nose.tools
import json
import time
import sys

from conf import const

def cleanup():                                                                                             
    ret = subprocess.Popen(const.teracli_binary + ' disable test',                                         
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)                     
    print ''.join(ret.stdout.readlines())                                                                  
    print ''.join(ret.stderr.readlines())                                                                  
    time.sleep(2)                                                                                          
    ret = subprocess.Popen(const.teracli_binary + ' drop test',                                            
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)                     
    print ''.join(ret.stdout.readlines())                                                                  
    print ''.join(ret.stderr.readlines())

    files = os.listdir('.')
    for f in files:
        if f.endswith('.out'):
            os.remove(f)  


def createbyfile(schema, deli=''):                                                                         
    """                                                                                                    
    This function creates a table according to a specified schema                                          
    :param schema: schema file path                                                                        
    :param deli: deli file path                                                                            
    :return: None                                                                                          
    """
    cleanup()                                                                                              
    create_cmd = '{teracli} createbyfile {schema} {deli}'.format(teracli=const.teracli_binary, 
                                                                 schema=schema, deli=deli)
    print create_cmd
    ret = subprocess.Popen(create_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())
    print ''.join(ret.stderr.readlines()) 


def run_tera_mark(file_path, op, table_name, random, value_size, num, key_size, cf='', key_seed=1, value_seed=1,
                  key_step=1, entry_limit=0):  
    """                                                                                                    
    This function provide means to write data into Tera and dump a copy into a specified file at the same time.
    :param file_path: a copy of data will be dumped into file_path for future use                          
    :param op: ['w' | 'd'], 'w' indicates write and 'd' indicates delete                                   
    :param table_name: table name                                                                          
    :param random: ['random' | 'seq']                                                                      
    :param value_size: value size in Bytes                                                                 
    :param num: entry number                                                                               
    :param key_size: key size in Bytes                                                                     
    :param cf: cf list, e.g. 'cf0:qual,cf1:flag'. Empty cf list for kv mode. Notice: no space in between   
    :param key_seed: seed for random key generator                                                         
    :param value_seed: seed for random value generator                                                     
    :param key_step: interval for key values
    :param entry_limit: the writing speed for limitation
    :return: None                                                                                          
    """   
    # write data into Tera                                                                                 
    tera_bench_args = ""
    awk_args = ""

    if cf == '':  # kv mode
        tera_bench_args += """--compression_ratio=1 --key_seed={kseed} --value_seed={vseed} """\           
                           """ --value_size={vsize} --num={num} --benchmarks={random} """\                 
                           """ --key_size={ksize} --key_step={kstep}""".format(kseed=key_seed,
                                                       vseed=value_seed, vsize=value_size, num=num,
                                                       random=random, ksize=key_size,
                                                       kstep=key_step) 

        if op == 'd':  # delete                                                                            
            awk_args += """-F '\t' '{print $1}'"""                                                         
        else:  # write
            awk_args += """-F '\t' '{print $1"\t"$2}'""" 
    else:  # table 
        tera_bench_args += """--cf={cf} --compression_ratio=1 --key_seed={kseed} --value_seed={vseed} """\
                           """ --value_size={vsize} --num={num} --benchmarks={random} """\                 
                           """ --key_size={ksize} --key_step={kstep}""".format(cf=cf,
                                                            kseed=key_seed, vseed=value_seed,
                                                            vsize=value_size, num=num, random=random, ksize=key_size,
                                                            kstep=key_step)

        if op == 'd':  # delete                                                                            
            awk_args += """-F '\t' '{print $1"\t"$3"\t"$4}'"""                                             
        else:  # write                                                                                     
            awk_args += """-F '\t' '{print $1"\t"$2"\t"$3"\t"$4}'"""  

    tera_mark_args = """--mode={op} --tablename={table_name} --type=async """\                             
                     """ --verify=false --entry_limit={elimit}""".format(op=op, table_name=table_name,     
                                                                   elimit=entry_limit) 

    cmd = '{tera_bench} {bench_args} | awk {awk_args} | {tera_mark} {mark_args}'.format(                   
           tera_bench=const.tera_bench_binary, bench_args=tera_bench_args, awk_args=awk_args,                 
           tera_mark=const.tera_mark_binary, mark_args=tera_mark_args)

    print cmd                                                                                              
    ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)                
    print ''.join(ret.stdout.readlines())                                                                  
    print ''.join(ret.stderr.readlines()) 

    # write/append data to a file for comparison                                                           
    for path, is_append in file_path:   
        if cf == '':                                                                                       
            awk_args = """-F '\t' '{print $1"::0:"$2}'"""                                                  
        else:                                                                                              
            awk_args = """-F '\t' '{print $1":"$3":"$4":"$2}'"""   

        redirect_op = ''                                                                                   
        if is_append is True:                                                                              
            redirect_op += '>>'                                                                            
        else:                                                                                              
            redirect_op += '>' 

        dump_cmd = '{tera_bench} {tera_bench_args} | awk {awk_args} {redirect_op} {out}'.format(           
                    tera_bench=const.tera_bench_binary, tera_bench_args=tera_bench_args,                           
                    redirect_op=redirect_op, awk_args=awk_args, out=path)                                          
        print dump_cmd 
        ret = subprocess.Popen(dump_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)       
        print ''.join(ret.stdout.readlines())                                                              
        print ''.join(ret.stderr.readlines())


def merge_file(old_f, new_f, out_f):                                                                       
                      
    def get_key(data):                                                                                     
        parts = line.split(":", 3)                                                                         
        parts.pop()                                                                                        
        row_cf_qualifier_key = ':'.join(parts)                                                             
        return row_cf_qualifier_key                                                                        

    # save new data's key in dict                                                                          
    new_keys_value = dict()                                                                                
    with open(new_f, "r") as f:                                                                            
        for line in f:                                                                                     
            k = get_key(line)
            new_keys_value[k] = line      
            
    with open(out_f, "w") as wf:                                                                           
        with open(old_f, "r") as f1:                                                                       
            for line in f1:                                                                                
                k = get_key(line)
                if k in new_keys_value:                                                                    
                    wf.write(new_keys_value[k])                                                            
                else:
                    wf.write(line) 


def rest_file(old_f, new_f, out_f):                                                                       
                      
    def get_key(data):                                                                                     
        parts = line.split(":", 3)                                                                         
        parts.pop()                                                                                        
        row_cf_qualifier_key = ':'.join(parts)                                                             
        return row_cf_qualifier_key                                                                        

    # save new data's key in dict                                                                          
    new_keys_value = dict()                                                                                
    with open(new_f, "r") as f:                                                                            
        for line in f:                                                                                     
            k = get_key(line)
            new_keys_value[k] = line      
            
    with open(out_f, "w") as wf:                                                                           
        with open(old_f, "r") as f1:                                                                       
            for line in f1:                                                                                
                k = get_key(line)
                if k in new_keys_value:                                                                    
                    continue                                                          
                else:
                    wf.write(line) 


def scan_table(table_name, file_path, allversion, snapshot=0):
    """
    This function scans the table and write the output into file_path
    :param table_name: table name
    :param file_path: write scan output into file_path                                                     
    :param allversion: [True | False]                                                                      
    """
    allv = ''
    if allversion is True:
        allv += 'scanallv'
    else:
        allv += 'scan'

    snapshot_args = ''                                                                                     
    if snapshot != 0:                                                                                      
        snapshot_args += '--snapshot={snapshot}'.format(snapshot=snapshot)
 
    scan_cmd = '{teracli} {op} {table_name} "" "" {snapshot} > {out}'.format(                              
            teracli=const.teracli_binary, op=allv, table_name=table_name, snapshot=snapshot_args, out=file_path)
    print scan_cmd                                                                                         
    ret = subprocess.Popen(scan_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)           
    print ''.join(ret.stdout.readlines())                                                           
    print ''.join(ret.stderr.readlines())


def compare_files(file1, file2, need_sort):                                                                
    """                                                                                                    
    This function compares two files.                                                                      
    :param file1: file path to the first file                                                              
    :param file2: file path to the second file                                                             
    :param need_sort: whether the files need to be sorted                                                  
    :return: True if the files are the same, False on the other hand                                       
    """                                                                                                    
    if need_sort is True:                                                                                  
        sort_cmd = 'sort {f1} > {f1}.sort; sort {f2} > {f2}.sort'.format(f1=file1, f2=file2)               
        print sort_cmd                                                                                     
        ret = subprocess.Popen(sort_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)       
        print ''.join(ret.stdout.readlines())                                                              
        print ''.join(ret.stderr.readlines())                                                              
        os.rename(file1+'.sort', file1)                                                                    
        os.rename(file2+'.sort', file2)                                                                    
    return filecmp.cmp(file1, file2, shallow=False)
