import re
from urlparse import urlparse
from collections import Counter
import gzip
#
import boto
import warc
#
from boto.s3.key import Key
from gzipstream import GzipStreamFile
from mrjob.job import MRJob

class CCJob(MRJob):
  def process_record(self, record):
    """
    Override process_record with your mapper
    """
    raise NotImplementedError('Process record needs to be customized')

  def mapper(self, _, line):
    f = None
    ## If we're on EC2 or running on a Hadoop cluster, pull files via S3
    if self.options.runner in ['emr', 'hadoop']:
      # Connect to Amazon S3 using anonymous credentials
      conn = boto.connect_s3(anon=True)
      pds = conn.get_bucket('aws-publicdatasets')
      # Start a connection to one of the WARC files
      k = Key(pds, line)
      f = warc.WARCFile(fileobj=GzipStreamFile(k))
    ## If we're local, use files on the local file system
    else:
      print 'Loading local file {}'.format(line)
      f = warc.WARCFile(fileobj=gzip.open(line))
    ###
    for i, record in enumerate(f):
      for key, value in self.process_record(record):
        yield key, value
      self.increment_counter('commoncrawl', 'processed_records', 1)

TWITTER_PATTERN = re.compile("twitter\.com\/[a-zA-Z0-9_]{1,15}\/?\Z")
YOUTUBE_PATTERN = re.compile("youtube\.com\/user\/[a-zA-Z0-9]{1,20}\/?\Z")
INSTAGRAM_PATTERN = re.compile("instagram\.com\/[a-zA-Z0-9_.]{1,30}\/?\Z")

class SocialSparkRegexJob(CCJob):
  def process_record(self, record):
    if record['Content-Type'] == 'application/http; msgtype=response':
      url = record["WARC-Target-URI"]

      found = False
      if INSTAGRAM_PATTERN.findall(url):
        found = True
      elif TWITTER_PATTERN.findall(url):
        found = True
      elif YOUTUBE_PATTERN.findall(url):
        found = True
      '''
      elif "facebook.com" in url and "facebook.com" in domain:
        found = True
      elif "soundcloud.com" in url and "soundcloud.com" in domain:
        found = True
      elif "twitch.tv" in url and "twitch.tv" in domain:
        found = True
      elif "pinterest.com" in url and "pinterest.com" in domain:
        found = True
      elif "vine.co" in url and "vine.co" in domain:
        found = True
      '''

      if found:
        payload = record.payload.read()
        headers, body = payload.split('\r\n\r\n', 1)
        domain = urlparse(url).netloc
        yield domain, tuple([{url:body}])
        self.increment_counter('commoncrawl', 'processed_pages', 1)

  def combiner(self, key, values):
    yield key, next(values)

  def reducer(self, key, values):
    yield key, next(values)

if __name__ == '__main__':
  SocialSparkRegexJob.run()
