"""
Created on 2014-1-26
@author: wangtaize@baidu.com
@copyright: www.baidu.com
"""
import os
import subprocess
import sys


USE_SHELL = sys.platform.startswith("win")
SVN_EXPORT_ERROR_CODE = 10000
TIMEOUT = 600


class ShellHelper(object):
    """
    shell helper
    """
    def __init__(self):
        self.is_win = sys.platform.startswith("win")

    def run_with_retuncode(self, command,
                                universal_newlines=True,
                                useshell=USE_SHELL,
                                env=os.environ):
        """run with return code
        """
        try:
            p = subprocess.Popen(command,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE,
                                  shell=useshell,
                                  universal_newlines=universal_newlines,
                                  env=env)
            output, errout = p.communicate()
            return p.returncode, output, errout
        except Exception:
            return -1, None, None

    def run_with_realtime_print(self, command,
                                universal_newlines=True,
                                useshell=USE_SHELL,
                                env=os.environ,
                                print_output=True):
        """run with realtime print
        """
        try:
            p = subprocess.Popen(command,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT,
                                  shell=useshell,
                                  env=env)
            for line in iter(p.stdout.readline, ''):
                if print_output:
                    sys.stdout.write(line)
                else:
                    sys.stdout.write('\r')
            sys.stdout.write('\r')
            p.wait()
            return p.returncode
        except Exception:
            return -1

    def run_with_interactive(self, command,
                                  useshell=USE_SHELL,
                                  env=os.environ):
        """run with interactive
        """
        try:
            p = subprocess.Popen(command,
                                  stdout=sys.stdout,
                                  stderr=sys.stderr,
                                  stdin=sys.stdin,
                                  shell=useshell,
                                  env=env)
            p.wait()
            return p.returncode
        except Exception:
            return -1

   

