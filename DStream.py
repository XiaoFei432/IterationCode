import pdb
import os


def MakeRDD(iterable):
    return RDD(iterable)

def TextFile(path, ext=""):

    def iter_files(paths):
        for path in paths:
            with open(path) as f:
                for line in f:
                    yield line
        raise StopIteration

    def iter_file(path):
        with open(path) as f:
            for line in f:
                yield line
        raise StopIteration

    path = os.path.realpath(path)
    if os.path.isdir(path):
        paths = []
        for f in os.listdir(path):
            if os.path.isfile(f):
                if ext:
                    if f.endswith(ext):
                        paths.append(f)
                else:
                    paths.append(f)
            else:
                pass
        return iter_files(paths)
    else:
        return iter_file(path)


class RDD(object):
    def __init__(self, source):
        self._source = iter(source)
        
    def __iter__(self):
        return (row for row in self._source)
    
    def map(self, func):
        return MappedRDD(self, func)

    def filter(self, func):
        return FilterRDD(self, func)

    def reduce(self, func):
        return reduce(func, self)

    def flatmap(self, func):
        return FlatMapRDD(self, func)

    def mapkey(self):
        pass

    def mapvalue(self):
        pass

    def groupBykey(self):
        pass

    def reduceBykey(self):
        pass

    def collect(self):
        return list(self)

    def collectAsmap(self):
        pass

    def sort(self, key=None, reverse=False):
        pass

    def unique(self):
        return UniqueRDD(self)

class MappedRDD(RDD):
    def __init__(self, source, func):
        RDD.__init__(self, source)
        self._mapfunc = func 
    
    def __iter__(self):
        return (self._mapfunc(row) for row in self._source)


class FilterRDD(RDD):
    def __init__(self, source, func):
        RDD.__init__(self, source)
        self._filterfunc = func

    def __iter__(self):
        return (row for row in self._source if self._filterfunc(row))

class FlatMapRDD(RDD):
    def __init__(self, source, func):
        RDD.__init__(self, source)
        self._mapfunc = func
    
    def __iter__(self):
        def flat(func, source):
            for row in source:
                for ele in func(row):
                    yield ele
            raise StopIteration
        return flat(self._mapfunc, self._source)

class UniqueRDD(RDD):
    def __init__(self, source):
        RDD.__init__(self, source)
    
    def __iter__(self):
        seen = set()
        def unique(source):
            for row in source:
                if row not in seen:
                    seen.add(row)
                    yield ele
            raise StopIteration

        return unique(self._source)


