#coding=utf-8
from qiniu import conf as qConf, rsf as qRsf
import logging
import urllib


class QBucketError(Exception):
    def __init__(self, message=None):
        Exception.__init__(self, "%s" % (message,))


class QBucket(object):
    def __init__(self, bucket, domain, accessKey, secretKey):
        if not (bucket and domain and accessKey and secretKey):
            raise QBucketError('')

        self.bucket = bucket
        self.domain = domain
        qConf.ACCESS_KEY = accessKey
        qConf.SECRET_KEY = secretKey

        self._urlopen = urllib.URLopener()

        self.initialize()
        return

    def initialize(self):
        return

    def listAll(self):
        client = qRsf.Client()
        itemList = []
        marker = 'begin'
        while marker:
            current_m = marker if marker != 'begin' else ''
            ret, err = client.list_prefix(self.bucket, prefix='', limit=1000, marker=current_m)
            if err and err != 'EOF':
                raise QBucketError('Error to get list use marker: %s' % (current_m,))
            itemList += ret['items']
            marker = ret.get('marker')
        items = dict()
        for i in itemList:
            items[i['key'].encode('utf8')] = str(i['hash'])
        return items

    def getFile(self, key, localPath):
        url = 'http://%s/%s' % (self.domain, urllib.quote(key))
        logging.debug('downloading: %s' % (url,))
        try:
            self._urlopen.retrieve(url, localPath)
            logging.debug('downloaded: %s' % (url,))
        except Exception as e:
            logging.warning('failed to download: Domain:%s ; Key:%s ; Exception:%s'
                            % (self.domain, key, e))
        return
