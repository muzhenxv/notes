#! /usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import collections
import datetime
import hashlib
import json
import logging
import numbers
import os
import re
import sys
import warnings
from collections import OrderedDict
from itertools import groupby
from math import log10

import joblib as pickle
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, StructType, StructField

warnings.filterwarnings("ignore", category=Warning)

spark = SparkSession.builder. \
    config("spark.driver.maxResultSize", "8g"). \
    config("spark.dynamicAllocation.enabled", "false"). \
    config("hive.exec.orc.split.strategy", "ETL"). \
    appName("user_registration_fraud_identification_spark"). \
    enableHiveSupport().getOrCreate()

nullContentList = {'unknown', 'null', 'nan', 'none', '(null)', '(nan)', '(none)', '未知', '', ' ', '0'}

if sys.version_info[0] >= 3:
    basestring = str
    unicode = str
    long = int


def _str2bool(str):
    return True if str.lower() == 'true' else False


def _hashfunc(x):
    return int(hashlib.md5(x).hexdigest(), 16)


def getParser():
    parser = argparse.ArgumentParser()

    parser.add_argument('--localPath', type=str, default='./', required=False, help='The localPath of input file')
    parser.add_argument('--hdfsPath', type=str, default='hdfs:///user/risk_deploy/singlepass_cluster_user_registration_fraud_identification/', required=False, help='The hdfsPath of output file')
    parser.add_argument('--colsArgsFile', type=str, default='colsArgs.json', required=False, help='The argument of input cols')

    parser.add_argument('--objectIdName', type=str, default='seqno', required=False, help='ClusterId = objectIdName + ClusterIdTail')
    parser.add_argument('--ClusterIdTail', type=str, default='dt', required=False, help='ClusterId = objectIdName + ClusterIdTail')

    parser.add_argument('--maxNearDupsSize', type=int, default=400, required=False, help='The maxSize of getNearDups')

    parser.add_argument('--weightThreshold', type=float, default=0.6, required=False, help='The weightThreshold of SinglePassCluster')
    parser.add_argument('--scoreThreshold', type=float, default=0.4, required=False, help='The scoreThreshold of SinglePassCluster')

    parser.add_argument('--clusterCntMin', type=float, default=2, required=False, help='The clusterCntMin for clusterscore')
    parser.add_argument('--clusterCntMax', type=int, default=20, required=False, help='The clusterCntMax for clusterscore')
    parser.add_argument('--simValueMin', type=float, default=0.6, required=False, help='The simValueMin for clusterscore')
    parser.add_argument('--simValueMax', type=float, default=2.5, required=False, help='The clusterCntMin for clusterscore')

    parser.add_argument('--simhashDim', type=int, default=64, required=False, help='The dimensions of fingerprints')
    parser.add_argument('--showFrequency', type=int, default=10000, required=False, help='The frequency to show SinglePassCluster cnt')
    args = parser.parse_known_args()[0]

    return args


def buildByFeatures(features, simhashDim):
    v = [0] * simhashDim
    masks = [1 << i for i in range(simhashDim)]
    if isinstance(features, dict):
        features = features.items()
    for f in features:
        if isinstance(f, basestring):
            h = _hashfunc(f.encode('utf-8'))
            w = 1
        else:
            assert isinstance(f, collections.Iterable)
            h = _hashfunc(f[0].encode('utf-8'))
            w = f[1]
        for i in range(simhashDim):
            v[i] += w if h & masks[i] else -w
    binaryStr = ''.join(['0' if i <= 0 else '1' for i in v[::-1]])
    return int(binaryStr, 2)


def Simhash(value, f=64, reg=r'[\w\u4e00-\u9fcc]+', hashfunc=None, log=None):
    if isinstance(value, collections.Iterable):
        return buildByFeatures(value, f)
    else:
        return ''


class SimhashIndex(object):

    def __init__(self, objs, f=64, k=2, log=None):
        """
        `objs` is a list of (objId, simhash)
        objId is a string, simhash is an instance of Simhash
        `f` is the same with the one for Simhash
        `k` is the tolerance
        """
        self.f = f
        count = len(objs)

        self.bucket = collections.defaultdict(dict)

        for i, q in enumerate(objs):
            self.add(*q)

    def getNearDups(self, simhash, simhashColName, maxNearDupsSize, k):
        """
        `simhash` is an instance of Simhash
        return a list of objId, which is in type of str
        """
        #         assert simhash.f == self.f

        ans = set()

        for key in self.getKeys(simhash, simhashColName, k):
            dups = self.bucket[key]

            for objId, sim2 in dups.items():
                sim2 = SimhashClass(long(sim2, 16), self.f)
                d = sim2.distance(simhash)
                if d <= k:
                    ans.add((objId, d))
                if len(ans) >= maxNearDupsSize:
                    break
        return list(ans)

    def add(self, objId, simhash, simhashColName, k):
        """
        `objId` is a string
        `simhash` is an instance of Simhash
        """
        assert simhash.f == self.f

        v = '%x' % simhash.value
        for key in self.getKeys(simhash, simhashColName, k):
            self.bucket[key][str(objId)] = v

    def delete(self, objId, simhash, simhashColName, k):
        """
        `objId` is a string
        `simhash` is an instance of Simhash
        """
        assert simhash.f == self.f

        v = '%x' % simhash.value
        for key in self.getKeys(simhash, simhashColName, k):
            if str(objId) in self.bucket[key].keys():
                self.bucket[key].pop(str(objId))

    def offsets(self, k):
        return [self.f // (k + 1) * i for i in range(k + 1)]

    def getKeys(self, simhash, simhashColName, k):
        if isinstance(simhash, SimhashClass):
            simhash = simhash.value
        else:
            simhash = int(simhash)

        for i, offset in enumerate(self.offsets(k)):
            if i == (len(self.offsets(k)) - 1):
                m = 2 ** (self.f - offset) - 1
            else:
                m = 2 ** (self.offsets(k)[i + 1] - offset) - 1
            c = simhash >> offset & m
            yield '%x:%x:%s' % (c, i, simhashColName)

    def bucketSize(self):
        return len(self.bucket)


class SimhashClass(object):

    def __init__(self, value, f=64, reg=r'[\w\u4e00-\u9fcc]+', hashfunc=None, log=None):
        """
        `f` is the dimensions of fingerprints

        `reg` is meaningful only when `value` is basestring and describes
        what is considered to be a letter inside parsed string. Regexp
        object can also be specified (some attempt to handle any letters
        is to specify reg=re.compile(r'\w', re.UNICODE))

        `hashfunc` accepts a utf-8 encoded string and returns a unsigned
        integer in at least `f` bits.
        """

        self.f = f
        self.reg = reg
        self.value = None

        if hashfunc is None:
            self.hashfunc = _hashfunc
        else:
            self.hashfunc = hashfunc

        if isinstance(value, SimhashClass):
            self.value = value.value
        elif isinstance(value, basestring):
            self.buildByText(unicode(value))
        elif isinstance(value, collections.Iterable):
            self.buildByFeatures(value)
        elif isinstance(value, numbers.Integral):
            self.value = value
        else:
            raise Exception('Bad parameter with type {}'.format(type(value)))

    def __eq__(self, other):
        """
        Compare two simhashes by their value.
        :param Simhash other: The Simhash object to compare to
        """
        return self.value == other.value

    def slide(self, content, width=4):
        return [content[i:i + width] for i in range(max(len(content) - width + 1, 1))]

    def tokenize(self, content):
        content = content.lower()
        content = ''.join(re.findall(self.reg, content))
        ans = self.slide(content)
        return ans

    def buildByText(self, content):
        features = self.tokenize(content)
        features = {k: sum(1 for _ in g) for k, g in groupby(sorted(features))}
        return self.buildByFeatures(features)

    def buildByFeatures(self, features):
        """
        `features` might be a list of unweighted tokens (a weight of 1
                   will be assumed), a list of (token, weight) tuples or
                   a token -> weight dict.
        """
        v = [0] * self.f
        masks = [1 << i for i in range(self.f)]
        if isinstance(features, dict):
            features = features.items()
        for f in features:
            if isinstance(f, basestring):
                h = self.hashfunc(f.encode('utf-8'))
                w = 1
            else:
                assert isinstance(f, collections.Iterable)
                h = self.hashfunc(f[0].encode('utf-8'))
                w = f[1]
            for i in range(self.f):
                v[i] += w if h & masks[i] else -w
        # use reversed binary str to keep the backward compatibility
        binaryStr = ''.join(['0' if i <= 0 else '1' for i in v[::-1]])
        self.value = int(binaryStr, 2)

    def distance(self, another):
        if isinstance(another, SimhashClass):
            value = another.value
        else:
            value = int(another)

        #         assert self.f == another.f
        x = (self.value ^ value) & ((1 << self.f) - 1)
        ans = 0
        while x:
            ans += 1
            x &= x - 1
        return ans


class SinglePassClusterSearcher(object):
    def __init__(self, args):
        self.args = args
        self.recordsCnt = 0
        self.objectIdName = args.objectIdName
        self.ClusterIdTail = args.ClusterIdTail

        self.nullContentList = nullContentList
        self.stopWords = ['~', '：', '/', '「', '、', '★', ',', '|', '【', '^', '」', '—', '“', '>', '.', '￥', '%', ')', '_', '……', ';', '（', '?', '&', '‘', ' ', '，', '-', '～', '）',
                          '！', '＃', '”', '!', '。', '#', '？', '{', 't', '；', '\\', '】', '+', '*', '《', '$', '…', '=', ']', '[', "'", '·', '(', '"', '’', ':', '》', '→', '<']

        logging.info("Loading colsArgs")
        try:

            localSrc = os.path.join(self.args.localPath, self.args.colsArgsFile)
            hdfsDst = os.path.join(self.args.hdfsPath, self.args.colsArgsFile)

            os.system("rm {0}".format(localSrc))
            os.system("hadoop fs -get {0} {1}".format(hdfsDst, localSrc))
            os.system("hadoop fs -put -f {0} {1}".format(localSrc, hdfsDst))
            self.colsArgs = json.load(open(file=localSrc, mode='r', encoding="utf-8"), object_pairs_hook=OrderedDict)
        except:
            logging.error("File {} does not exist. Exit.".format(self.args.colsArgsFile))
            exit(1)

        self.simhashColsIndexDict = {key: value["colIndex"] for key, value in self.colsArgs.items() if value["simhashFlag"] > 0}
        self.coreWordColsIndexDict = {key: value["colIndex"] for key, value in self.colsArgs.items() if value["coreWordFlag"] > 0}
        self.colsCutStepDict = {key: value["cutStep"] for key, value in self.colsArgs.items() if value["cutStep"] > 0}

        self.colsMinContentLenDict = {key: value["minContentLen"] for key, value in self.colsArgs.items() if value["minContentLen"] > 0}
        self.simhashColsTolDict = {key: value["tolerance"] for key, value in self.colsArgs.items() if value["tolerance"] > 0}
        self.colsWeightDict = {key: value["weight"] for key, value in self.colsArgs.items() if value["weight"] > 0}

        self.keyWordsList = [key for key, value in self.colsArgs.items() if value["keyWordsFlag"] > 0]
        self.keyWordsCntTemp = 0

        self.colOrder = [key for key, value in self.colsArgs.items()]

        logging.info("Initialization")
        self.colsSimhashIndex = SimhashIndex((), f=self.args.simhashDim)
        self.clustersRecordDict = collections.defaultdict(dict)
        self.uid2clusterIdDict = collections.defaultdict(dict)
        self.clustersCnt = len(self.clustersRecordDict)

    def getStopWords(self, file):
        stopwords = []
        stopwords.append(' ')
        for word in open(file, "r"):
            stopwords.append(word.encode('utf-8').decode('utf-8').strip())
            stopwords = list(set(stopwords))

        return stopwords

    def getCutContent(self, content, step):
        cutContentData = []

        # 当列内容为长度为 2 以内时，不参与切片处理，直接返回
        if len(content) <= step:
            return content
        else:
            # 其他情况列内容去除停用词，然后切片处理
            content = [word for word in content if word not in self.stopWords]
        cutContentLower = ''.join(content).lower()
        cutContentData.append([cutContentLower[i:i + step] for i in range(len(cutContentLower) - step + 1)])

        return cutContentData[0]

    def UpdateCoreWord(self, row, clusterId):
        if 'coreword' not in self.clustersRecordDict[clusterId].keys():
            self.clustersRecordDict[clusterId]['coreword'] = collections.defaultdict(dict)

        for ColName, value in row[self.coreWordColsIndexDict.keys()].items():
            if ColName not in self.clustersRecordDict[clusterId]['coreword'].keys():
                self.clustersRecordDict[clusterId]['coreword'][ColName] = {}

            if value not in self.clustersRecordDict[clusterId]['coreword'][ColName].keys():
                self.clustersRecordDict[clusterId]['coreword'][ColName][value] = 1
            else:
                self.clustersRecordDict[clusterId]['coreword'][ColName][value] += 1

    def search(self, objectIdName, ClusterIdTail, candiUidsWithWeightSorted):
        # 有相似成员
        if len(candiUidsWithWeightSorted) > 0 and self.clustersCnt != 0:
            if str(objectIdName) in [key for (key, v) in candiUidsWithWeightSorted]:
                objectFlag = 'repeatObject'
                bestClusterId = self.uid2clusterIdDict[objectIdName]
                bestClusterValue = self.args.weightThreshold
                clusterCnt = self.clustersRecordDict[bestClusterId]['memberscnt']

                return bestClusterId, clusterCnt, bestClusterValue, objectFlag
            elif candiUidsWithWeightSorted[0][1] >= self.args.weightThreshold:
                objectFlag = 'memberObject'
                bestClusterId = self.uid2clusterIdDict[str(candiUidsWithWeightSorted[0][0])]
                bestClusterValue = candiUidsWithWeightSorted[0][1]
                self.clustersRecordDict[bestClusterId]['memberscnt'] += 1
                clusterCnt = self.clustersRecordDict[bestClusterId]['memberscnt']
                self.uid2clusterIdDict[objectIdName] = bestClusterId

                return bestClusterId, clusterCnt, bestClusterValue, objectFlag

        objectFlag = 'newObject'
        bestClusterId = objectIdName + "_" + ClusterIdTail
        bestClusterValue = self.args.weightThreshold
        self.clustersRecordDict[bestClusterId]['memberscnt'] = 1

        clusterCnt = self.clustersRecordDict[bestClusterId]['memberscnt']
        self.uid2clusterIdDict[objectIdName] = bestClusterId

        self.clustersCnt += 1

        return bestClusterId, clusterCnt, bestClusterValue, objectFlag

    def clusterScore(self, clusterCnt, simValue):
        simValueMin = self.args.simValueMin
        simValueMax = self.args.simValueMax
        clusterSimScoreMin = 0.0
        clusterSimScoreHeight = 0.9

        clusterCntMin = self.args.clusterCntMin
        clusterCntMax = self.args.clusterCntMax
        clusterCntScoreMin = 0.0
        clusterCntScoreHeight = 1.1

        if simValue < simValueMax:
            clusterSimScore = round((clusterSimScoreMin + clusterSimScoreHeight * (max(0, simValue - simValueMin) / (simValueMax - simValueMin)) ** (0.25)), 2)  # 0.5
        else:
            clusterSimScore = clusterSimScoreMin + clusterSimScoreHeight

        if clusterCnt < clusterCntMax:
            clusterCntScore = round((clusterCntScoreMin + clusterCntScoreHeight * (max(0, clusterCnt - clusterCntMin) / (clusterCntMax - clusterCntMin)) ** (0.25)), 2)  # 0.5
        else:
            clusterCntScore = clusterCntScoreMin + clusterCntScoreHeight

        clusterScore = float('%.2f' % np.average([clusterSimScore, clusterCntScore], weights=[1, 1]))
        return clusterScore

    def pickleDump(self, path, data):
        f = open(path, 'wb')
        pickle.dump(data, f)
        f.close()

    def clusterResult(self, inparams_source, inparams_simhash):
        clusterResult = collections.defaultdict(dict)
        self.keyWordsCntTemp = 0

        inparams_source_df = pd.DataFrame.from_dict(inparams_source, orient='index').T
        inparams_simhash_df = pd.DataFrame.from_dict(inparams_simhash, orient='index').T

        inparams_source_df = inparams_source_df[self.colOrder]
        inparams_simhash_df = inparams_simhash_df[self.colOrder]

        inparams_source = inparams_source_df.iloc[0, :]
        inparams_simhash = inparams_simhash_df.iloc[0, :]

        clusterResult['inparams'] = OrderedDict(inparams_source)
        clusterResult[self.objectIdName] = str(inparams_source[self.objectIdName])

        candiUidsWithWeight = {}
        for simhashColName, inputStrCutSimhash in inparams_simhash[self.simhashColsIndexDict.keys()].items():
            if len(str(inputStrCutSimhash)) >= self.colsMinContentLenDict[simhashColName]:
                colCandiUidsList = self.colsSimhashIndex.getNearDups(inputStrCutSimhash, simhashColName, self.args.maxNearDupsSize, k=int(self.simhashColsTolDict[simhashColName]))

                for candiUid, distance in colCandiUidsList:
                    if candiUid not in candiUidsWithWeight.keys():
                        candiUidsWithWeight[candiUid] = self.colsWeightDict[simhashColName] * max(0, 1 - (0.15 * distance))
                    else:
                        candiUidsWithWeight[candiUid] += self.colsWeightDict[simhashColName] * max(0, 1 - (0.15 * distance))

                Simhash_t = SimhashClass([''], f=self.args.simhashDim)
                Simhash_t.value = int(inputStrCutSimhash)
                self.colsSimhashIndex.add(clusterResult[self.objectIdName], Simhash_t, simhashColName, k=int(self.simhashColsTolDict[simhashColName]))

        candiUidsWithWeightSorted = sorted(candiUidsWithWeight.items(), key=lambda item: item[1], reverse=True)
        clusterResult['clusterid'], clusterResult['clustercnt'], simValue, objectFlag = self.search(clusterResult[self.objectIdName], str(inparams_source[self.ClusterIdTail]),
                                                                                                    candiUidsWithWeightSorted)

        if objectFlag == 'repeatObject':
            if 'simmean' not in self.clustersRecordDict[clusterResult['clusterid']].keys():
                self.clustersRecordDict[clusterResult['clusterid']]['simmean'] = simValue
            clusterResult['simmean'] = float('%.2f' % self.clustersRecordDict[clusterResult['clusterid']]['simmean'])
            clusterResult['clusterscore'] = self.clusterScore(clusterResult['clustercnt'], clusterResult['simmean'])

            clusterResult['coreword'] = {}
            for coreWordColName in self.clustersRecordDict[clusterResult['clusterid']]['coreword'].keys():
                coreWordWithCnt = sorted(self.clustersRecordDict[clusterResult['clusterid']]['coreword'][coreWordColName].items(), key=lambda item: item[1], reverse=True)[0]
                clusterResult['coreword'][coreWordColName] = (str(coreWordWithCnt[0]), float('%.4f' % (coreWordWithCnt[1] / clusterResult['clustercnt'])))

            clusterResult['reason'] = "The object is strongly like a repeat user"
        elif objectFlag == 'memberObject':
            self.UpdateCoreWord(inparams_source, clusterResult['clusterid'])

            clusterResult['coreword'] = {}
            for coreWordColName in self.clustersRecordDict[clusterResult['clusterid']]['coreword'].keys():
                coreWordWithCnt = sorted(self.clustersRecordDict[clusterResult['clusterid']]['coreword'][coreWordColName].items(), key=lambda item: item[1], reverse=True)[0]
                clusterResult['coreword'][coreWordColName] = (str(coreWordWithCnt[0]), float('%.4f' % (coreWordWithCnt[1] / clusterResult['clustercnt'])))
                if (clusterResult['clustercnt'] > 1 and int(clusterResult['coreword'][coreWordColName][1]) == 1) and coreWordColName in self.keyWordsList:
                    self.keyWordsCntTemp += 1

            if self.keyWordsCntTemp == len(self.keyWordsList):
                if 'simmean' not in self.clustersRecordDict[clusterResult['clusterid']].keys():
                    self.clustersRecordDict[clusterResult['clusterid']]['simmean'] = simValue
                clusterResult['simmean'] = float('%.2f' % self.clustersRecordDict[clusterResult['clusterid']]['simmean'])
                clusterResult['clusterscore'] = self.clusterScore(clusterResult['clustercnt'] - 1, clusterResult['simmean'])

                clusterResult['reason'] = "The object is strongly like a repeat user"
            else:
                if 'simmean' not in self.clustersRecordDict[clusterResult['clusterid']].keys():
                    self.clustersRecordDict[clusterResult['clusterid']]['simmean'] = simValue
                else:
                    simValueOld = self.clustersRecordDict[clusterResult['clusterid']]['simmean']
                    # 更新均值
                    self.clustersRecordDict[clusterResult['clusterid']]['simmean'] = simValueOld + (simValue - simValueOld) / clusterResult['clustercnt']

                clusterResult['simmean'] = float('%.2f' % self.clustersRecordDict[clusterResult['clusterid']]['simmean'])
                clusterResult['clusterscore'] = self.clusterScore(clusterResult['clustercnt'], clusterResult['simmean'])

                clusterResult['reason'] = "The object is strongly correlated with {0} other objects.".format(clusterResult['clustercnt'] - 1)
                clusterResult['reason'] += "Their top correlated features and the percentages of the objects sharing the same features are as follows: "
                for key, value in clusterResult['coreword'].items():
                    clusterResult['reason'] += "{0}% of \"{1}\" is like \"{2}\", ".format(float('%.4f' % (float(value[1]) * 100)), key, str(value[0]))
        else:
            self.UpdateCoreWord(inparams_source, clusterResult['clusterid'])
            if 'simmean' not in self.clustersRecordDict[clusterResult['clusterid']].keys():
                self.clustersRecordDict[clusterResult['clusterid']]['simmean'] = simValue
            clusterResult['simmean'] = float('%.2f' % self.clustersRecordDict[clusterResult['clusterid']]['simmean'])
            clusterResult['clusterscore'] = self.clusterScore(clusterResult['clustercnt'], clusterResult['simmean'])

            clusterResult['coreword'] = {}
            for coreWordColName in self.clustersRecordDict[clusterResult['clusterid']]['coreword'].keys():
                coreWordWithCnt = sorted(self.clustersRecordDict[clusterResult['clusterid']]['coreword'][coreWordColName].items(), key=lambda item: item[1], reverse=True)[0]
                clusterResult['coreword'][coreWordColName] = (str(coreWordWithCnt[0]), float('%.4f' % (coreWordWithCnt[1] / clusterResult['clustercnt'])))

            clusterResult['reason'] = "The object is strongly correlated with 0 other objects."

        self.recordsCnt += 1
        return clusterResult

    def executeModel(self, inContent_source, inContent_simhash):
        inparams_source = json.loads(inContent_source, object_pairs_hook=OrderedDict)
        inparams_simhash = json.loads(inContent_simhash, object_pairs_hook=OrderedDict)
        try:
            if str(inparams_source[self.objectIdName]).lower() in self.nullContentList:
                logging.info('The id is null\n')

                clusterResultDict = collections.defaultdict(dict)
                clusterResultDict['inparams'] = OrderedDict(inparams_source)
                clusterResultDict[self.objectIdName] = str(inparams_source[self.objectIdName])
                clusterResultDict['reason'] = "The objectId is null."
                self.recordsCnt += 1
                clusterResultDict['clusterscore'] = 0.0

            else:
                clusterResultDict = self.clusterResult(inparams_source, inparams_simhash)

            return clusterResultDict
        except BaseException as e:
            logging.error("%s" % e)

            clusterResultDict = collections.defaultdict(dict)
            clusterResultDict['inparams'] = OrderedDict(inparams_source)
            clusterResultDict[self.objectIdName] = str(inparams_source[self.objectIdName])
            clusterResultDict['clusterscore'] = 0.0
            clusterResultDict['reason'] = "reason%s" % e
            self.recordsCnt += 1

            return clusterResultDict

    def getCorewordStr(self, item):
        coreword = []
        for k_sub, v_sub in item['coreword'].items():
            itemCoreword = sorted([(k_sub + ":" + k.replace(',', '.'), v) for k, v in v_sub.items() if str(k).lower() not in self.nullContentList], reverse=True)
            if len(itemCoreword) > 0:
                coreword.append(itemCoreword[0])
        sortedCoreword = sorted(coreword, key=lambda item: item[1], reverse=True)[0:min(3, len(coreword))]
        return ', '.join([item[0] for item in sortedCoreword])

    def getGeohashCode_udf(self):
        def getGeohashCode(LatColName, LonColName):
            lat, lng = LatColName, LonColName
            if len({str(lat).lower(), str(lng).lower()} & nullContentList) == 0:
                geohashCode = geohash.encode(float(lat), float(lng), precision=8)
            else:
                geohashCode = 'nan'
            return geohashCode

        return udf(lambda device_lat, device_lon: getGeohashCode(device_lat, device_lon), StringType())

    def getConcatFeature_udf(self):
        def getConcatFeature(ColName_1, ColName_2):
            content_1 = str(ColName_1).replace(',', '.')
            content_2 = str(ColName_2).replace(',', '.')

            if content_1.lower() in nullContentList:
                content_1 = ''
            if content_2.lower() in nullContentList:
                content_2 = ''

            concatFeature = content_1 + '_' + content_2

            return concatFeature if len(concatFeature) != 0 else 'nan'

        return udf(lambda ColName_1, ColName_2: getConcatFeature(ColName_1, ColName_2), StringType())

    def getSubString_udf(self, start, lenght):
        def getSubString(colName, start, lenght):
            content = str(colName)

            if content.lower() in nullContentList:
                subContent = 'nan'
            else:
                subContent = content[start:min(start + lenght, len(content))]
            return subContent

        return udf(lambda colName: getSubString(colName, start, lenght), StringType())

    def getCutContent_udf(self, step):
        def getCutContent(content, step):
            cutContentData = []
            if str(content).lower() in nullContentList:
                return 'nan'
            elif len(content) <= step:
                return content
            else:
                content = [word for word in content]

            cutContentLower = ''.join(content).lower()
            cutContentData.append([cutContentLower[i:i + step] for i in range(len(cutContentLower) - step + 1)])

            return cutContentData[0]

        return udf(lambda content: getCutContent(content, step))

    def getHashCode_udf(self, colsMinContentLen, simhashDim):
        def getHashCode(colsMinContentLen, simhashDim, inparamsCut):
            if len(str(inparamsCut)) >= colsMinContentLen:
                inputStrCutSimhash = Simhash(inparamsCut, f=simhashDim)
            else:
                inputStrCutSimhash = ''
            return inputStrCutSimhash

        return udf(lambda colContent: getHashCode(colsMinContentLen, simhashDim, colContent))


class Geohash(object):
    def __init__(self):

        self.base32 = '0123456789bcdefghjkmnpqrstuvwxyz'
        self.decodemap = {}
        for i in range(len(self.base32)):
            self.decodemap[self.base32[i]] = i
        del i

    def decode_exactly(self, geohash):
        """
        Decode the geohash to its exact values, including the error
        margins of the result.  Returns four float values: latitude,
        longitude, the plus/minus error for latitude (as a positive
        number) and the plus/minus error for longitude (as a positive
        number).
        """
        lat_interval, lon_interval = [-90.0, 90.0], [-180.0, 180.0]
        lat_err, lon_err = 90.0, 180.0
        is_even = True
        for c in geohash:
            cd = self.decodemap[c]
            for mask in [16, 8, 4, 2, 1]:
                if is_even:  # adds longitude info
                    lon_err /= 2
                    if cd & mask:
                        lon_interval = ((lon_interval[0] + lon_interval[1]) / 2, lon_interval[1])
                    else:
                        lon_interval = (lon_interval[0], (lon_interval[0] + lon_interval[1]) / 2)
                else:  # adds latitude info
                    lat_err /= 2
                    if cd & mask:
                        lat_interval = ((lat_interval[0] + lat_interval[1]) / 2, lat_interval[1])
                    else:
                        lat_interval = (lat_interval[0], (lat_interval[0] + lat_interval[1]) / 2)
                is_even = not is_even
        lat = (lat_interval[0] + lat_interval[1]) / 2
        lon = (lon_interval[0] + lon_interval[1]) / 2
        return lat, lon, lat_err, lon_err

    def decode(self, geohash):
        """
        Decode geohash, returning two strings with latitude and longitude
        containing only relevant digits and with trailing zeroes removed.
        """
        lat, lon, lat_err, lon_err = self.decode_exactly(geohash)
        # Format to the number of decimals that are known
        lats = "%.*f" % (max(1, int(round(-log10(lat_err)))) - 1, lat)
        lons = "%.*f" % (max(1, int(round(-log10(lon_err)))) - 1, lon)
        if '.' in lats: lats = lats.rstrip('0')
        if '.' in lons: lons = lons.rstrip('0')
        return lats, lons

    def encode(self, latitude, longitude, precision=8):
        """
        Encode a position given in float arguments latitude, longitude to
        a geohash which will have the character count precision.
        """
        lat_interval, lon_interval = [-90.0, 90.0], [-180.0, 180.0]

        geohash = []
        bits = [16, 8, 4, 2, 1]
        bit = 0
        ch = 0
        even = True
        while len(geohash) < precision:
            if even:
                mid = (lon_interval[0] + lon_interval[1]) / 2
                if longitude > mid:
                    ch |= bits[bit]
                    lon_interval = (mid, lon_interval[1])
                else:
                    lon_interval = (lon_interval[0], mid)
            else:
                mid = (lat_interval[0] + lat_interval[1]) / 2
                if latitude > mid:
                    ch |= bits[bit]
                    lat_interval = (mid, lat_interval[1])
                else:
                    lat_interval = (lat_interval[0], mid)
            even = not even
            if bit < 4:
                bit += 1
            else:
                geohash += self.base32[ch]
                bit = 0
                ch = 0
        return ''.join(geohash)


pt = sys.argv[1]
two_weeks_datetime = datetime.datetime.strptime(pt, '%Y%m%d') - datetime.timedelta(days=7)
timedelta_2weeks = two_weeks_datetime.strftime("%Y%m%d")

# pt = '20201001'
# timedelta_2weeks = '20201001'

sqlStr = '''
    select * from
    (
        select signer_up_table.seqno
            , signer_up_table.user_mobile
            , signer_up_table.device_hash
            , signer_up_table.device_id
            , signer_up_table.device_lon
            , signer_up_table.device_lat
            , signer_up_table.remote_ip
            , signer_up_table.bluetooth_address
            , signer_up_table.wifimac
            , signer_up_table.device_brand
            , signer_up_table.device_type
            , signer_up_table.appversion
            , signer_up_table.battery_level
            , signer_up_table.network
            , signer_up_table.imsi
            , signer_up_table.ssid_name
            , signer_up_table.root
            , signer_up_table.create_time
            , signer_up_table.dt
            , signer_success_table.uid
        from
        (
            --  获取注册申请用户
            select * from 
            (
                select seqno
                    , get_json_object(riskparams,'$._riskId') as riskId
                    , get_json_object(riskparams,'$.userMobile') as user_mobile
                    , get_json_object(riskparams,'$.deviceHash') as device_hash
                    , get_json_object(riskparams,'$.deviceId') as device_id
                    , get_json_object(riskparams,'$.deviceLon') as device_lon
                    , get_json_object(riskparams,'$.deviceLat') as device_lat
                    , get_json_object(riskparams,'$.remoteIp') as remote_ip  
                    , get_json_object(riskparams,'$.deviceFingerprinting_p') as bluetooth_address
                    , get_json_object(riskparams,'$.deviceFingerprinting_m') as wifimac

                    , get_json_object(riskparams,'$.deviceFingerprinting_c') as device_brand
                    , get_json_object(riskparams,'$.deviceFingerprinting_i') as device_type
                    , get_json_object(riskparams,'$.deviceFingerprinting_g') as appversion
                    , get_json_object(riskparams,'$.batteryLevel') as battery_level
                    , get_json_object(riskparams,'$.network') as network 
                    , get_json_object(riskparams,'$.deviceFingerprinting_f') as imsi
                    , get_json_object(riskparams,'$.ssidName') as ssid_name
                    , get_json_object(riskparams,'$.deviceFingerprinting_r') as root
                    , get_json_object(riskparams,'$.createTime') as create_time
                    , pt as dt
                    --, row_number() over(partition by seqno order by logtime) as rank_num
                from odssub.flowctrl_record
                where pt between {0} and {1} 
                and get_json_object(riskparams,'$.action') = '1'
                and eventcode = 'user_signup_event'
                and get_json_object(riskparams,'$.systemCode') in ('61','62')
            )
            --where rank_num = 1
        )signer_up_table
        left join
        (
            -- 获取用户注册成功，uid
            select * from 
            (
                select get_json_object(data,'$.uid') uid
                    , get_json_object(data,'$.userMobile') usermobile
                    , get_json_object(data,'$._riskId') riskId
                    --, row_number() over(partition by get_json_object(data,'$.uid') order by logtime) as rank_num
                from odssub.rcp_async_data
                where pt between {0} and {1} 
                and eventcode='user_signup_data_event'
                and get_json_object(data,'$.userMobile') is not  null
            )
        --where rank_num = 1
        )signer_success_table
        on cast(signer_up_table.user_mobile as bigint) = cast(signer_success_table.usermobile as bigint) and signer_up_table.riskId = signer_success_table.riskId
    )signer_success_with_uid_pay_order_table
    order by create_time
    '''.format(timedelta_2weeks, pt)

try:
    logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
    logging.root.setLevel(level=logging.INFO)
    args = getParser()
    geohash = Geohash()
    singlePass = SinglePassClusterSearcher(args)
    logging.info('spark sql start query')
    searchData = spark.sql(sqlStr)
    if len(searchData.take(1)) > 0:
        try:
            logging.info('spark sql end query')
            # dataPreprocess
            searchData = searchData.withColumn('geohash_code', singlePass.getGeohashCode_udf()(col('device_lat'), col('device_lon')))
            searchData = searchData.drop('device_lat')
            searchData = searchData.drop('device_lon')

            searchData = searchData.withColumn('device_brand_type', singlePass.getConcatFeature_udf()(col('device_brand'), col('device_type')))
            searchData = searchData.drop('device_brand')
            searchData = searchData.drop('device_type')

            searchData = searchData.withColumn('device_brand_type_appversion', singlePass.getConcatFeature_udf()(col('device_brand_type'), col('appversion')))
            searchData = searchData.drop('device_brand_type')
            searchData = searchData.drop('appversion')

            searchData = searchData.withColumn('create_time_hour', singlePass.getSubString_udf(0, 13)(searchData['create_time']))
            searchData = searchData.drop('create_time')

            searchData = searchData.withColumn('cover_phone_number', singlePass.getSubString_udf(0, 9)(searchData['user_mobile']))
            searchData = searchData.drop('user_mobile')

            searchData = searchData.select(singlePass.colOrder)
            searchData.cache()
            searchData_source = searchData.toPandas()

            # getCutContent
            searchData = searchData.select(
                ['seqno', 'battery_level', 'network', 'root'] + [singlePass.getCutContent_udf(singlePass.colsCutStepDict[str(column)])(col(column)).alias(str(column)) for column in
                                                                 singlePass.simhashColsIndexDict.keys()] + ['dt', 'uid'])
            searchData = searchData.select(
                ['seqno', 'battery_level', 'network', 'root'] + [singlePass.getHashCode_udf(singlePass.colsMinContentLenDict[str(column)], singlePass.args.simhashDim)(col(column)).alias(str(column))
                                                                 for column in singlePass.simhashColsIndexDict.keys()] + ['dt', 'uid'])
            searchData = searchData.select(singlePass.colOrder)
            searchData_simhash = searchData.toPandas()

            clusterScoreResult = []
            for index, row in searchData_source.iterrows():
                inContent_source = searchData_source.iloc[index, :].to_json()
                inContent_simhash = searchData_simhash.iloc[index, :].to_json()
                clusterResultDict = singlePass.executeModel(inContent_source, inContent_simhash)
                
                if index % singlePass.args.showFrequency == 0:
                    logging.info("index: %d" % index)
                    
                if clusterResultDict['clusterscore'] > singlePass.args.scoreThreshold and singlePass.keyWordsCntTemp != len(singlePass.keyWordsList):
                    clusterResultJson = json.dumps(clusterResultDict, ensure_ascii=False)
                    clusterScoreResult.append([clusterResultDict[singlePass.objectIdName], clusterResultDict['clusterid'], clusterResultDict['clusterscore'], clusterResultJson])

            logging.info('saving clustersResult\n')
            clustersResultTrainDF = pd.DataFrame(clusterScoreResult, columns=[singlePass.objectIdName, 'clusterid', 'clusterscore', 'inputoutputparams'])
            schema = StructType([StructField(col, StringType(), True) for col in clustersResultTrainDF.columns])
            clustersResultTrainDF = spark.createDataFrame(clustersResultTrainDF, schema=schema)

            clustersResultTrainDF.registerTempTable("table1")
            sqlStr = "insert overwrite table risk.user_registration_fraud_identification_clusters_result_v1 partition(pt={0}) select * from table1".format(pt)
            spark.sql(sqlStr)

            logging.info('saving uid2clusterIdDict\n')
            uid2clusterIdDF = pd.DataFrame([(k, v) for k, v in singlePass.uid2clusterIdDict.items()], columns=[str(singlePass.objectIdName), 'clusterid'])
            schema = StructType([StructField(col, StringType(), True) for col in uid2clusterIdDF.columns])
            uid2clusterIdDF = spark.createDataFrame(uid2clusterIdDF, schema=schema)

            uid2clusterIdDF.registerTempTable("table1")
            sqlStr = "insert overwrite table risk.user_registration_fraud_identification_uid2clusterid_v1 partition(pt={0}) select * from table1".format(pt)
            spark.sql(sqlStr)

            logging.info('saving clustersRecordDict\n')
            clustersRecordDF = pd.DataFrame([(k, v['memberscnt'], v['simmean'], singlePass.getCorewordStr(v)) for k, v in singlePass.clustersRecordDict.items()],
                                            columns=['clusterid', 'memberscnt', 'simmean', 'coreword'])
            schema = StructType([StructField(col, StringType(), True) for col in clustersRecordDF.columns])
            clustersRecordDF = spark.createDataFrame(clustersRecordDF, schema=schema)

            clustersRecordDF.registerTempTable("table1")
            sqlStr = "insert overwrite table risk.user_registration_fraud_identification_clusters_record_v1 partition(pt={0}) select * from table1".format(pt)
            spark.sql(sqlStr)

            logging.info('saving colsSimhashIndex\n')
            colsSimhashIndexDF = pd.DataFrame([(k, json.dumps(v, ensure_ascii=False)) for k, v in singlePass.colsSimhashIndex.bucket.items()], columns=['simhashkey', 'valueset'])
            schema = StructType([StructField(col, StringType(), True) for col in colsSimhashIndexDF.columns])
            colsSimhashIndexDF = spark.createDataFrame(colsSimhashIndexDF, schema=schema)

            colsSimhashIndexDF.registerTempTable("table1")
            sqlStr = "insert overwrite table risk.user_registration_fraud_identification_cols_simhash_index_v1 partition(pt={0}) select * from table1".format(pt)
            spark.sql(sqlStr)
        except Exception as e:
            logging.info(e)
            exit(1)
except Exception as e:
    logging.info('searchData isEmpty')
    exit(0)
