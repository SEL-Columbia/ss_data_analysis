# base.py
  
import fnmatch
import os


def field_map(dictseq, name, func):
    for d in dictseq:
        d[name] = func(d[name])
        yield d


def gen_cat(sources):
    for source in sources:
        for line in source:
            yield line.strip()


def gen_open(filenames):
    for name in filenames:
        with open(name, 'r') as f:
            yield f


def gen_find(filepat, top):
    for path, dirlist, filelist in os.walk(top):
        for name in fnmatch.filter(filelist, filepat):
            yield os.path.join(path, name)
