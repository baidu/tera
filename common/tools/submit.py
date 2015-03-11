#!/usr/bin/env python

from optparse import OptionParser
import hashlib
import getpass
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib
import xml.dom.minidom

'''
For security, submit configuration should be saved as web page
in remote server, in order to avoid abusers overstep submission
checkings via modifying the submit script codes.
'''
CONFIG_URL = "file:/home/anqin/bin/submit.xml" #"http://some_secure_server:port/submit_sciprt_configuration"
CODEREVIEW_SERVER = "http://code_review_server:port/uri"
READABILITY_URL = "http://web_page_of_configuration_for_verified_reviewers_list"

option_parser = OptionParser(usage="usage: %prog [options] (-i ISSUE | -s SUBJECT)")
option_parser.add_option("-i", "--issue", dest="issue", default=0,
                         help="codereview issue number.")
option_parser.add_option("-s", "--subject", dest="subject", action="store", type="string",
                         help="subject of submission issue.")
option_parser.add_option("-l", "--local_disk", action="store_false", dest="build_local", default=True,
                         help="blade in local disk, default in local.")
option_parser.add_option("-d", "--dry_run", action="store_true", dest="dry_run", default=False,
                         help="Do not actually submit any files; just print what would happen.")
option_parser.add_option("-q", "--quiet", action="store_true", dest="quiet", default=False,
                         help="Do not ask any question during submit.")
options = None

class SubmitConfiguration:
    def __init__(self):
        self.check_list = []
        self.exclude_list = []
        self.reviewers = set()
        self.coverage_standards = {}

    def load(self, config_url, local_path):
        try:
            dom = xml.dom.minidom.parseString(urllib.urlopen(config_url).read())

            # match the best dir in config file
            target_node = None
            target_path = ""
            for dir_node in dom.getElementsByTagName('dir'):
                path = dir_node.getAttribute('path').rstrip('/')
                if local_path.startswith(path) and path.startswith(target_path):
                    target_node = dir_node
                    target_path = path

            check_node = target_node.getElementsByTagName("check")[0]
            if check_node:
                self.check_list = [ node.firstChild.nodeValue.rstrip('/')
                        for node in check_node.getElementsByTagName('include') ]
                self.exclude_list = [ node.firstChild.nodeValue.rstrip('/')
                        for node in check_node.getElementsByTagName('exclude') ]
            readability_node = target_node.getElementsByTagName("readability")[0]
            if readability_node:
                self.reviewers = set([ node.firstChild.nodeValue.rstrip('/')
                        for node in readability_node.getElementsByTagName("reviewer")])

            coverage_node = target_node.getElementsByTagName("coverage")[0]
            if coverage_node:
                for node in coverage_node.getElementsByTagName("standard"):
                    location = node.getElementsByTagName("location")[0].firstChild.nodeValue
                    value = node.getElementsByTagName("value")[0].firstChild.nodeValue
                    self.coverage_standards[location.rstrip('/')] = int(value)
        except Exception, e:
            print >>sys.stderr, e
            print >>sys.stderr, "fail to parse remote configuation."
            self.check_list = [local_path]

    def need_check(self, path):
        for i in self.exclude_list:
            if path.startswith(i):
                return False
        return True

    def coverage_standard(self, path):
        entry = ""
        for i in self.coverage_standards:
            if path.startswith(i) and i.startswith(entry):
                entry = i
        if entry != "":
            return self.coverage_standards[entry]
        else:
            return 80

config = SubmitConfiguration()

#-----------------------------------------------------------------------------
#>>>>>>                           utils                                 <<<<<<
#-----------------------------------------------------------------------------

def _real_path(path):
    if os.path.islink(path):
        return os.readlink(path)
    else:
        return path

def _svn_status(svn_path):
    svn_status = subprocess.Popen(["svn", "info", svn_path],
                                  stdout=subprocess.PIPE).communicate()[0]
    return "\n".join(svn_status.split("\n")[1:])

def _svn_remote_path(svn_status):
    for i in svn_status.split("\n"):
        if i.startswith("URL: "):
            return i[5:].strip().partition('://')[2].rstrip('/')
    return ""

def _svn_revision(svn_status):
    for i in svn_status.split("\n"):
        if i.startswith("Last Changed Rev: "):
            return i
    return ""

def _check_svn_status(local_path):
    local_svn_status = _svn_status(local_path)
    remote_path = _svn_remote_path(local_svn_status)
    return (remote_path and
            _svn_revision(local_svn_status) ==
            _svn_revision(_svn_status('https://' + remote_path)))

def _issue_url(issue):
    return "%s/%s" % (CODEREVIEW_SERVER, issue)

def _patch_url(issue, patchset, patch):
    return "%s/download/issue%s_%s_%s.diff" % (CODEREVIEW_SERVER, issue, patchset, patch)

def _issue_info(issue):
    issue_info_url = "%s/%s/info" % (CODEREVIEW_SERVER, issue)
    issue_info = json.loads(urllib.urlopen(issue_info_url).read())
    return issue_info

def _issue_api(issue):
    issue_api_url = "%s/api/%s" % (CODEREVIEW_SERVER, issue, )
    issue_api = json.loads(urllib.urlopen(issue_api_url).read())
    return issue_api

def _patchset_api(issue, patchset):
    patchset_api_url = "%s/api/%s/%s" %  (CODEREVIEW_SERVER, issue, patchset)
    patchset_api = json.loads(urllib.urlopen(patchset_api_url).read())
    return patchset_api

def _blade_libraries(source_list):
    libraries = {}
    for path in source_list:
        build_file = os.path.join(os.path.dirname(path), 'BUILD')
        if os.path.isdir(path) or not os.path.exists(build_file):
            continue

        def cc_library(**keywords):
            if 'name' not in keywords or 'srcs' not in keywords:
                return
            basename = os.path.basename(path)
            srcs = keywords['srcs']
            if (isinstance(srcs, str) and srcs == basename) or basename in srcs:
                name = os.path.dirname(path) + ':' + keywords['name']
                if not name in libraries:
                    libraries[name] = set()
                libraries[name].add(basename)
        def dummy_rule(**keywords):
            pass
        blade_dummy_list = [
            'cc_binary', 'cc_test', 'proto_library', 'gen_rule',
            'lex_yacc_library', 'resource_library'
        ]
        methods = locals()
        for dummy in blade_dummy_list:
            methods[dummy]= dummy_rule
        execfile(build_file, globals(), methods)
    return libraries

def _local_change_list(check_list):
    include_status_set = set(["A", "M", "D"])
    change_list = {}
    for svn_path in check_list:
        svn_status = subprocess.Popen(
            ["svn", "status", svn_path], stdout = subprocess.PIPE).communicate()[0]
        for file_status in svn_status.splitlines():
            if len(file_status) <= 8 or file_status[0] not in include_status_set:
                continue
            status = file_status[0]
            path = file_status[8:]
            if os.path.isdir(path):
                # codereview does not include dir in most cases, so we skip it
                continue
            remote_path = _svn_remote_path(_svn_status(path))
            change_list[path] = {
                'status':      status,
                'remote_path': remote_path,
            }
    return change_list

def _remote_file_content(svn_base, file, patch_url = ""):
    # download patch
    patch_content = urllib.urlopen(patch_url).read()

    # patch source
    (fd, path) = tempfile.mkstemp("source")
    m = re.search(r'--- %s\s+\(revision (\d+)\)' % file, patch_content)
    if m != None and m.group(1) != "0":
        revision = m.group(1)
        subprocess.Popen(["svn", "cat", "-r", revision, svn_base + file],
                         stdout = fd).communicate()
    os.close(fd)
    subprocess.Popen(["patch", path], stdin = subprocess.PIPE, stderr = subprocess.PIPE,
                     stdout = subprocess.PIPE).communicate(patch_content)
    content = open(path).read()
    os.remove(path)
    return content

def _guess_local_path(svn_url):
    global config
    svn_url = svn_url.partition('://')[2].rstrip('/')
    svn_bases = [(_svn_remote_path(_svn_status(path)), path) for path in config.check_list]
    for svn_base, path in svn_bases:
        if svn_url.startswith(svn_base):
            return os.path.join(path, svn_url.partition(svn_base)[2].lstrip('/'))
    return ""

def _remote_change_list(issue):
    '''
    1) access the remote codereview server
    2) check the change list for specific issue
    3) get the modified file list
    '''
    return {}

def _coverage(build_dir, check_list):
    change_list = _local_change_list(check_list)
    library_list = _blade_libraries(change_list)
    results = {}
    for library, srcs in library_list.items():
        path, target = library.split(':')
        for name in srcs:
            gcno_file = os.path.join(build_dir, path, target + ".objs", name + ".gcno")
            source_file = os.path.join(path, name)
            p = subprocess.Popen(['gcov', '-n', '-o', gcno_file, source_file],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            outputs = p.communicate()[0].split("\n\n")
            for output in outputs:
                lines = output.split("\n")
                if lines[0] != "File '%s'" % source_file:
                    continue
                regex = r'Lines executed:(\d+[.]\d+)% of (\d+)'
                match = re.search(regex, lines[1])
                if match:
                    results[source_file] = {
                        'percent':  float(match.group(1)),
                        'lines':    int(match.group(2)),
                    }
    return results

def _get_reviewers_with_readability():
    readability_reviewers = set()
    readability_url = READABILITY_URL
    readability_reviewers = set()
    for line in urllib.urlopen(readability_url).read().splitlines():
      line = line.strip()
      if line.startswith("<li>"):
        name = line.replace("<li>", "").replace("</li>", "").strip()
        readability_reviewers.add(name)
    return readability_reviewers

def _switch_to_blade_root():
    current_dir = os.environ.get('PWD')
    blade_root_dir = current_dir
    if blade_root_dir.endswith('/'):
        blade_root_dir = blade_root_dir[:-1]
    while blade_root_dir and blade_root_dir != "/":
        if os.path.isfile(os.path.join(blade_root_dir, "BLADE_ROOT")):
            break
        blade_root_dir = os.path.dirname(blade_root_dir)
    if not blade_root_dir or blade_root_dir == "/":
        print >>stderr, "Can't find the BLADE_ROOT. Are you using blade?\n"
    os.chdir(blade_root_dir)
    return os.path.relpath(current_dir, blade_root_dir)


#-----------------------------------------------------------------------------
#>>>>>>                         check routines                          <<<<<<
#-----------------------------------------------------------------------------

check_status = {}

# decorator, log the return value of check routine
def check_routine(name):
    def make_check_routine_wrapper(f):
        def check_routine_wrapper(*args, **kwds):
            global check_status
            ret = f(*args, **kwds)
            if ret == None:
                check_status[name] = "skip"
            elif ret == True:
                check_status[name] = "pass"
            else:
                check_status[name] = "fail"
            return ret
        return check_routine_wrapper
    return make_check_routine_wrapper

# decorator, exit if check fails
def exit_check(name):
    def make_exit_check_wrapper(f):
        def exit_check_wrapper(*args, **kwds):
            ret = f(*args, **kwds)
            if ret == False:
                check_string = name.replace('_', ' ')
                print >>sys.stderr, "Check %s failed, exit." % check_string
                sys.exit(-1)
            return ret
        return exit_check_wrapper
    return make_exit_check_wrapper

# decorator, ask user what to do if check fails
def tolerant_check(name):
    def make_tolerant_check_wrapper(f):
        def tolerant_check_wrapper(*args, **kwds):
            ret = f(*args, **kwds)
            if ret == False:
                check_string = name.replace('_', ' ')
                global options
                if options.quiet:
                    print >>sys.stderr, "Check %s failed, exit." % check_string
                    sys.exit(-1)
                msg = "Check %s failed, do you want to continue? [y/N] " % check_string
                cont = raw_input(msg)
                if cont.lower() not in ["yes", "y"]:
                    sys.exit(-1)
            return ret
        return tolerant_check_wrapper
    return make_tolerant_check_wrapper

# decorator, let user decide whether take the check
def skipable_check(name):
    def make_skipable_check_wrapper(f):
        def skipable_check_wrapper(*args, **kwds):
            global options
            if not options.quiet:
                check_string = name.replace('_', ' ')
                skip = raw_input("Do you want to check %s? [Y/n] " % check_string)
                if skip.lower() in ["no", "n"]:
                    return None
            return f(*args, **kwds)
        return skipable_check_wrapper
    return make_skipable_check_wrapper

@check_routine('review_state')
@exit_check('review_state')
def check_review_state(issue):
    '''
    1) get the verified reviewers from $READABILITY_URL
    2) check the approval status for specific issue
    3) if all approval are given, return true
    4) if not all approval are received, return false
    '''
    return True

@check_routine('svn_up_to_date')
@exit_check('svn_up_to_date')
def check_svn_up_to_date(check_list):
    if not all([_check_svn_status(_real_path(i)) for i in check_list]):
        print >>sys.stderr, "Your codebase is not up-to-date, please svn update to head."
        return False
    return True

@check_routine('coding_style')
@exit_check('coding_style')
def check_coding_style(local_change_list):
    target_subfixs = ['cc', 'h']
    check_list = [path for path in local_change_list
            if path.rpartition('.')[2] in target_subfixs and config.need_check(path)]
    if len(check_list) == 0:
        return True
    p = subprocess.Popen(["common/tools/cpplint.py"] + check_list, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    output = p.communicate()[1]
    if p.returncode != 0:
        print >>sys.stderr, "You have some problems with coding style, please fix these first.\n%s" % output
        return False
    return True

@check_routine('consistency')
@tolerant_check('consistency')
def check_consistency(local_change_list, remote_change_list):
    '''
    1) get local change list and remote change list
    2) check whether both of them are same
       for each change in local list
          if in remote list:
              check whether diff result shows same
              if not, return false
          else
              return false
       return true
    '''
    return True

@check_routine('compile')
@exit_check('compile')
def check_compile(build_dir, check_list):
    blade_targets = [path + "/..." for path in check_list]
    os.system("find %s -name *.gcda -exec rm '{}' ';'" % build_dir)
    p = subprocess.Popen(["blade", "build", "--generate-dynamic", "--gcov"] + blade_targets)
    status = os.waitpid(p.pid, 0)[1]
    return status == 0

@check_routine('unit_tests')
@skipable_check('unit_tests')
@exit_check('unit_tests')
def check_unit_tests(build_dir, check_list):
    blade_targets = [path + "/..." for path in check_list]
    p = subprocess.Popen(["blade", "test",
        "--full-test", "--generate-dynamic", "--gcov", "--test-jobs=4"] + blade_targets)
    status = os.waitpid(p.pid, 0)[1]
    return status == 0

@check_routine('coverage')
@tolerant_check('coverage')
def check_coverage(build_dir, check_list):
    coverage = _coverage(build_dir, check_list)
    ret = True
    for source_file, data in coverage.items():
        if data['lines'] != 0 and data['percent'] < config.coverage_standard(source_file):
            print >>sys.stderr, "%s's coverage rate is only %d%%, which should be %d%%." % (
                    source_file, data['percent'], config.coverage_standard(source_file))
            ret = False
        else:
            print "%s's coverage rate: %d%%" % (source_file, data['percent'])
    return ret

#-----------------------------------------------------------------------------
#>>>>>>                         main routines                           <<<<<<
#-----------------------------------------------------------------------------

def need_build(check_list):
    for path in _local_change_list(check_list):
        if config.need_check(path):
            return True
    return False

def wait_until_tmpfs_available():
    while (len(glob.glob("/mnt/tmpfs/submit_cache/*")) > 3):
        print "submit queue is full, wait 10s to check again..."
        time.sleep(10)

def prepare_build_dir():
    global options
    if options.build_local:
        build_dir = "build64_submit"
    else:
        wait_until_tmpfs_available()
        build_dir = "/mnt/tmpfs/submit_cache/%s" % getpass.getuser()

    if not os.path.exists(build_dir):
        os.mkdir(build_dir)

    try:
        os.rename("build64_release", "build64_release.bak")
        os.symlink(build_dir, "build64_release")
    except:
        pass

    return build_dir

def clear_build_dir(build_dir):
    global options
    if os.path.exists("build64_release.bak"):
        os.remove("build64_release")
        os.rename("build64_release.bak", "build64_release")
    if not options.build_local:
        shutil.rmtree(build_dir)

def run_checks():
    local_change_list = _local_change_list(config.check_list)
    remote_change_list = _remote_change_list(options.issue)
    if not options.dry_run:
        check_review_state(options.issue)
    check_consistency(local_change_list, remote_change_list)
    check_svn_up_to_date(config.check_list)
    check_coding_style(local_change_list)
    if need_build(local_change_list):
        build_dir = prepare_build_dir()
        try:
            check_compile(build_dir, config.check_list)
            if check_unit_tests(build_dir, config.check_list) == True:
                check_coverage(build_dir, config.check_list)
        except KeyboardInterrupt:
            print >>sys.stderr, "submit is canceled by user."
            sys.exit(-1)
        finally:
            clear_build_dir(build_dir)

def close(issue):
    print "please close %s" % _issue_url(issue)
    pass

def submit(issue, subject, submit_list, dry_run):
    if issue > 0:
      issue_url = _issue_url(issue)
      issue_info = _issue_info(issue)
      title = issue_info["subject"]
    else:
      issue_url = "None"
      if len(subject) > 0:
        title = subject
      else:
        print >>sys.stderr, "submit need an issue no or subject."
        return

    global check_status
    check_report = ','.join(["%s->%s" % (item[0], item[1]) for item in check_status.items()])

    (fd, path) = tempfile.mkstemp("submit_svn_commit")
    f = open(path, "w")
    print >>f, title.encode('UTF-8')
    print >>f, "Issue: %s" % issue_url
    print >>f, "Check: %s" % check_report
    m = hashlib.md5()
    m.update(title.encode('UTF-8'))
    m.update(issue_url)
    m.update(check_report)
    print >>f, "Digest: %s" % m.hexdigest()
    f.close()

    submit_targets = ' '.join([_real_path(t) for t in submit_list])
    if dry_run:
        print >>sys.stdout, "svn commit --encoding UTF-8 %s -F %s" % (submit_targets, path)
        print >>sys.stdout, open(path).read()
        p = subprocess.Popen(["svn status %s" % submit_targets], shell = True)
    else:
        p = subprocess.Popen(["svn commit --encoding UTF-8 %s -F %s" % (submit_targets, path)], shell = True)
    status = os.waitpid(p.pid, 0)[1]
    os.remove(path)

def main():
    global options
    global config

    options = option_parser.parse_args()[0]
    local_path = _switch_to_blade_root()
    if local_path == ".":
        print >>sys.stdout, "please do not run submit.py under blade root, run in a subdir."
        sys.exit(-1)
    config.load(CONFIG_URL, local_path)

    run_checks()
    submit(options.issue, options.subject, config.check_list, options.dry_run)
    close(options.issue)

if __name__ == "__main__":
    main()
