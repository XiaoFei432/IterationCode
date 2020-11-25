#coding=utf-8
#usually used 

import operator
import codecs 
import pdb

class RDD:
    def __init__(self):
        #_source must be iterable
        self._source = None

    def __init__(self, source):
        self._source = source

    def __getitem__(self, ind):
        r = self._source[ind]
        return r

    @property
    def source():
        return _source

    def union(self, rdd_se):
        result = [] 
        for ele in self._source:
            result.append(ele)
        for ele in rdd_se.source:
            result.append(ele)
        return RDD(result)
    
    def map(self, parse_ele):
        result = []
        for ele in self._source:
            #pdb.set_trace()
            r = parse_ele(ele)
            if r == '':
                continue
            if r == 0:
                result.append(r)
            if r:
                result.append(r)
        self._source = result
        return self

    def map_two(self, parse_ele, another):
        result = []
        for ele in self._source:
            r = parse_ele(ele, another)
            if r:
                result.append(r)
        self._source = result
        return self
    
    def imap(self, rdd, func_two):
        result = []
        for ind, ele in enumerate(self._source):
            r = func_two(ele, rdd[ind])
            result.append(r)
        self._source = result
        return self
    #it assums every elem must be tuple or iterator
    def flatmap(self, parse_ele):
        result = []
        for row in self._source:
            for e in row:
                result.append(func(e))

        self._source = result       
        return self

    #it assums every elem must be two tuple
    def mapvalue(self):
        result = []
        for ele in self._source:
            key, value = ele
            result.append(value)
        
        self._source = result
        return self
   
    #it assums every elem must be two tuple 
    def mapkey(self):
        result = []
        for ele in self._source:
            key, value = ele
            result.append(key)

        self._source = result
        return self

    #no key
    def reduce(self, func):
        it = iter(self._source)
        accum_value = next(it)
        for ele in it:
            accum_value = func(accum_value, ele)
        return accum_value

    #it assums every elem must be two tuple
    def reducebykey(self, func):
        r = dict()
        for ele in self._source:
            key,value = ele
            if r.has_key(key):
                accum_value = func(r[key], value)
                r[key] = accum_value
            else:
                r[key] = value
        
        self._source = r.items()
        return self

    #it assums every ele must be two tuple
    def collectAsmap(self):
        r = dict()
        for ele in self._source:
            key,value = ele
            r[key] = value
        return r
    
    def collect(self):
        r = []
        for ele in self._source:
            r.append(ele)
        return r

    def sort(self, key=None, reverse=False):
        self._source.sort(key=key, reverse=reverse)
        return self

    def unique(self):
        result_set = set()
        for ele in self._source:
            result_set.add(ele)

        self._source  = [x for x in result_set]
        return self

    def filter(self, filter_ele):
        result = []
        for ele in self._source:
            if filter_ele(ele):
                result.append(ele)
        self._source = result
        return self

    #ele must be two tuple(key, value)
    def groupbykey(self):
        d = dict()
        for ele in self._source:
            key,value = ele
            d.setdefault(key, [])
            d[key].append(value)

        result = d.items()
        self._source = result
        return self

    def groupby(self, parse_key):
        d = dict()
        for ele in self._source:
            key = parse_key(ele)
            d.setdefault(key, [])
            d[key].append(value)
        
        result = d.items()
        self._source = result
        return self
            

    def saveAstxtfile(self, filename):
        fw = open(filename, 'w')
        for ele in self._source:
            if ele:
                fw.write(ele)
        fw.close()

    def saveAstxtgbkfile(self, filename):
        fw = codecs.open(filename, 'w', 'gbk')
        #ele must be str type
        for ele in self._source:
            fw.write(ele)
        fw.close()

    #count the ele occur times
    def count(self):
        d = dict()
        for ele in self._source:
            d.setdefault(ele, 0)
            d[ele] += 1
        self._source = d.items()
        return self



    @staticmethod
    def txtfile(filename):
        f = open(filename, 'r')
        return RDD(f)

    @staticmethod
    def txtfolder(folder):
        result = []
        from os import listdir
        from os.path import join, isfile
        for file in ( join(folder,f) for f in listdir(folder) if isfile(join(folder,f)) ):
            with open(file) as fo:
                for line in fo:
                    result.append(line)

        return RDD(result)

    @staticmethod
    def txtgbkfile(filename):
        f = codecs.open(filename, 'r', 'gbk')
        return RDD(f)
