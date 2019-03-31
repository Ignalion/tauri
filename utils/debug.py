import os
import subprocess
import sys


def pstats2png(file_path, remove=False):
    assert os.path.isfile(file_path)
    tempfile = os.path.join(os.path.dirname(file_path), 'tmp.pstats.dot')
    gprof_cmd = ['/Users/ignalion/anaconda/bin/gprof2dot', '-f', 'pstats', '-o', tempfile, file_path]
    dot_cmd = ['dot', '-T', 'png', '-o', file_path + '.png', tempfile]
    subprocess.check_call(gprof_cmd)
    subprocess.check_call(dot_cmd, shell=bool(sys.platform == 'win32'))
    os.remove(tempfile)
    if remove:
        os.remove(file_path)
    return file_path + '.png'
