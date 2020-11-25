import os
import sys

class RDD(object):
    def __init__(self):
        pass

    def iterator(self):
        return self.compute()

    def compute(self):
        raise NotImplementedError
    
    def map(self, func):
        return MappedRDD(self, func)

    def filter(self, func):
        return FilterRDD(self, func)

    def union(self, rdd):
        return UnionRDD(self, rdd)

    def zip(self, rdd):
        return ZipRDD(self, rdd)

    def reduce(self, func):
        return reduce(func, self.iterator())
    
    def reduceBykey(self, func):
        return ReduceBykeyRDD(self, func)

    def flatmap(self, func):
        return FlatMapRDD(self, func)

    def mapkey(self):
        return MappedKeyRDD(self)

    def mapvalue(self):
        return MappedValueRDD(self)

    def groupBykey(self):
        return GroupBykeyRDD(self) 

    def grouBy(self, func):
        return GroupByRDD(self, func)

    def collect(self):
        return list(self.iterator())

    def collectAsmap(self):
        d = {}
        for key,value in self.iterator():
            d[key] = value
        return d

    def sort(self, key=None, reverse=False):
        return SortedRDD(self, key, reverse)

    def unique(self):
        return UniqueRDD(self)

    def sample(self, index=0, size=10):
        return SampleRDD(self, index, size)

    @classmethod
    def TextFile(cls, path, withKey=False):
        return InputTexFileRDD(path, withKey)

    @classmethod
    def fromStream(cls, withKey=False):
        return FromStreamRDD(withKey)


    @classmethod
    def makeRDD(cls, rdd):
        return MakeRDD(rdd)

    def saveAsTextFile(self, outpath):
        return OutputTextFileRDD(self, outpath).collect()

    def saveAsStream(self):
        return OutputStreamRDD(self).collect()


class DrivedRDD(RDD):
    def __init__(self, rdd):
        RDD.__init__(self)
        self.prev = rdd


class MakeRDD(DrivedRDD):
    def __init(self, rdd):
        DrivedRDD.__init__(self, rdd)
    
    def compute(self):
        return iter(self.prev)

class FromStreamRDD(DrivedRDD):
    def __init__(self, withKey):
        DrivedRDD.__init__(self, None)
        self.withKey = withKey

    def compute(self):
        def fromStream(withKey):
            for line in sys.stdin:
                yield line 
        return fromStream(self.withKey)


class InputTexFileRDD(DrivedRDD):
    def __init__(self, path, withKey):
        DrivedRDD.__init__(self, None)
        self.path = path
        self.withKey = withKey

    def compute(self):
        def TextFile(path, withKey, ext=""):

            def iter_files(paths, withKey):
                if withKey:
                    for k, path in enumerate(paths):
                        with open(path) as f:
                            for i, line in enumerate(f):
                                yield (k*100000000+i, line)
                else:
                    for k,path in enumerate(paths):
                        with open(path) as f:
                            for line in f:
                                yield line

                raise StopIteration
            
            def iter_file(path, withKey):
                with open(path) as f:
                    if withKey:
                        for i,line in enumerate(f):
                            yield (i,line)
                    else:
                        for line in f:
                            yield line

                raise StopIteration
            path = os.path.realpath(path)
            if os.path.isdir(path):
                paths = []
                for f in os.listdir(path):
                    if os.path.isfile(os.path.join(path, f)):
                        if ext:
                            if f.endswith(ext):
                                paths.append(os.path.join(path, f))
                        else:
                            paths.append(os.path.join(path, f))
                    else:
                        pass
                return iter_files(paths, withKey)
            else:
                return iter_file(path, withKey)
        
        return TextFile(self.path, self.withKey)

class OutputTextFileRDD(DrivedRDD):
    def __init__(self, rdd, filename):
        DrivedRDD.__init__(self, rdd)
        self.filename = filename

    def compute(self):
        with open(self.filename, 'w') as f:
            lines = self.prev.iterator()
            for line in lines:
                if line.endswith('\n'):
                    f.write(line)
                else:
                    f.write(line)
                    f.write('\n')
        
        yield self.filename

class OutputStreamRDD(DrivedRDD):
    def __init__(self, rdd):
        DrivedRDD.__init__(self, rdd)

    def compute(self):
        lines = self.prev.iterator()
        for line in lines:
            if line.endswith('\n'):
                print line,
            else:
                print line

        yield None



class MappedRDD(DrivedRDD):
    def __init__(self, rdd, func):
        DrivedRDD.__init__(self, rdd)
        self._mapfunc = func 
    
    def compute(self):
        def generator(source):
            for row in source:
                r = self._mapfunc(row)
                if r:
                    yield r
                if r==0:
                    yield r
            raise StopIteration
        return generator(self.prev.iterator())


class FilterRDD(DrivedRDD):
    def __init__(self, rdd, func):
        DrivedRDD.__init__(self, rdd)
        self._filterfunc = func

    def compute(self):
        return (row for row in self.prev.iterator() if self._filterfunc(row))

class FlatMapRDD(DrivedRDD):
    def __init__(self, rdd, func):
        DrivedRDD.__init__(self, rdd)
        self._mapfunc = func
    
    def compute(self):
        def flat(func, source):
            for row in source:
                for ele in row:
                    yield func(ele)
            raise StopIteration
        return flat(self._mapfunc, self.prev.iterator())

class UnionRDD(DrivedRDD):
    def __init__(self, rdd, rdd_another):
        DrivedRDD.__init__(self, None)
        self.first_rdd = rdd
        self.second_rdd = rdd_another
    
    def compute(self):
        def generator(fi_source, se_source):
            for ele in fi_source:
                yield ele
            for ele in se_source:
                yield ele
            raise StopIteration
        return generator(self.first_rdd.iterator(), self.second_rdd.iterator())

class ZipRDD(DrivedRDD):
    def __init__(self, rdd, rdd_another):
        DrivedRDD.__init__(self, None)
        self.first_rdd = rdd
        self.second_rdd = rdd_another
    
    def compute(self):
        def generator(fi_source, se_source):
            try:
                while True:
                    item_fi = next(fi_source)
                    item_se = next(se_source)
                    yield (item_fi, item_se)
            except StopIteration:
                pass
            finally:
                raise StopIteration

        return generator(self.first_rdd.iterator(), self.second_rdd.iterator())


class MappedValueRDD(DrivedRDD):
    def __init__(self, rdd):
        RDD.__init__(self, rdd)
    
    def compute(self):
        return (value for key, value in self.prev.iterator())

class MappedKeyRDD(DrivedRDD):
    def __init__(self, rdd):
        RDD.__init__(self, rdd)
    
    def compute(self):
        return (key for key, value in self.prev.iterator())

class UniqueRDD(DrivedRDD):
    def __init__(self, rdd):
        DrivedRDD.__init__(self, rdd)
    
    def compute(self):

        def unique(source):
            seen = set()
            for row in source:
                if row not in seen:
                    seen.add(row)
                    yield row
            raise StopIteration

        return unique(self.prev.iterator())


class GroupBykeyRDD(DrivedRDD):
    def __init__(self, rdd):
        DrivedRDD.__init__(self, rdd)
    
    def compute(self):
        def groupBykey(source):
            d = dict()
            for key,value in source:
                d.setdefault(key, [])
                d[key].append(value)
            return d.iteritems()

        return groupBykey(self.prev.iterator())


class GroupByRDD(DrivedRDD):
    def __init__(self, rdd, func):
        DrivedRDD.__init__(self, rdd)
        self.func = func
    
    def compute(self):

        def groupby(source, func):
            d = dict()
            for row in source:
                key = func(row)
                d.setdefault(key, [])
                d[key].append(row)
            return d.itervalues()

        return groupby(self.prev.iterator(), self.func)

class ReduceBykeyRDD(DrivedRDD):
    def __init__(self, rdd, func):
        DrivedRDD.__init__(self, rdd)
        self.func = func
    
    def compute(self):
        def reduceBykey(source, func):
            d = dict()
            for key,value in source:
                d.setdefault(key, [])
                d[key].append(value)
            
            for key, value in d.iteritems():
                yield (key, reduce(func, value))

        return reduceBykey(self.prev.iterator(), self.func)
 
class SortedRDD(DrivedRDD):
    def __init__(self, rdd, key, reverse):
        DrivedRDD.__init__(self, rdd)
        self.key = key
        self.reverse = reverse
    
    def compute(self):

        def generator(source, key, reverse):
            result = sorted(source, key=key, reverse=reverse)
            for row in result:
                yield row
            raise StopIteration

        return generator(self.prev.iterator(), self.key, self.reverse)

class SampleRDD(DrivedRDD):
    def __init__(self, rdd, index, size):
        DrivedRDD.__init__(self, rdd)
        self.index = index
        self.size = size
    
    def compute(self):

        def generator(source, index, size):
            count = 0
            for row in source:
                count += 1
                if count > index and count < index + size + 1:
                    yield row
                if count > index + size:
                    break

            raise StopIteration

        return generator(self.prev.iterator(), self.index, self.size)


