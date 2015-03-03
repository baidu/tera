#!/usr/bin/env python

import os
import sys
import commands
import shutil

SRC_DIR="./"
DEST_DIR=""

def NeedCopy(src_dir, dest_dir, file_name):
  filename = file_name[1:]
  src_path = "%s%s" %(src_dir, filename)
  dest_path = "%s%s" %(dest_dir, filename)
  if not os.path.exists(dest_path):
    return True
  diff_cmd = "diff %s %s" %(src_path, dest_path)
  output = commands.getoutput(diff_cmd)
  if len(output) > 0 and len(output.split("\n")) > 1:
    return True
  return False

def AddSvn(path):
  cmd = "svn add %s" %path
  output = commands.getoutput(cmd)

def Copy(src_dir, dest_dir, file_name):
  filename = file_name[1:]
  src_path = "%s%s" %(src_dir, filename)
  dest_path = "%s%s" %(dest_dir, filename)
  need_svn = False
  if not os.path.exists(dest_path):
    need_svn = True
    dest_dir = os.path.dirname(dest_path)
    if not os.path.exists(dest_dir):
      print "mkdir: %s" %dest_dir
      os.makedirs(dest_dir)
      AddSvn(dest_dir)
  print "Copy: %s ==> %s" %(src_path, dest_path)
  shutil.copy(src_path, dest_path)
  if need_svn:
    AddSvn(dest_path)

def GetAllFile(src_dir, dest_dir):
  cmd = "cd %s && find . -name '*.h' -or -name '*.cpp' -or -name BUILD -or -name '*.proto' -or -name '*.cc'" %src_dir
  output = commands.getoutput(cmd)
  for source_file in output.split("\n"):
    if NeedCopy(src_dir, dest_dir, source_file):
      # print "need copy: %s" %source_file
      Copy(src_dir, dest_dir, source_file)

def Usage(prg):
  print "%s   <src_dir>  <dest_dir>"

def real_main(argv):
  if len(argv) < 3:
    Usage(argv[0])
    return
  GetAllFile(argv[1], argv[2])

def main():
  real_main(sys.argv)

if __name__ == "__main__":
  main()
