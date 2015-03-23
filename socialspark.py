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

SOCIAL_PROFILE_REGEX = re.compile("|".join([
  "twitter\.com\/[a-zA-Z0-9_]{1,15}\/?\Z",
  "youtube\.com\/user\/[a-zA-Z0-9]{1,20}\/?\Z",
  "instagram\.com\/[a-zA-Z0-9_.]{1,30}\/?\Z",
  "vine\.co\/[a-zA-Z0-9_.]{1,30}\/?\Z",
  "pinterest\.com\/[a-zA-Z0-9_.]{1,30}\/?\Z",
  "soundcloud\.com\/[a-zA-Z0-9_.\-]{1,30}\/?\Z",
  "twitch\.tv\/[a-zA-Z0-9_.\-]{1,30}\/profile\/?\Z"
]))

VALID_DOMAIN_REGEX = re.compile("|".join([
  'twitter\.com',
  'youtube\.com',
  'instagram\.com',
  'vine\.co',
  'pinterest\.com',
  'soundcloud\.com',
  'twitch\.tv'
]))

class SocialSparkRegexJob(CCJob):
  def process_record(self, record):
    if record['Content-Type'] == 'application/http; msgtype=response':
      url = record["WARC-Target-URI"]
      domain = urlparse(url).netloc

      if VALID_DOMAIN_REGEX.findall(domain) and SOCIAL_PROFILE_REGEX.findall(url):
        payload = record.payload.read()
        headers, body = payload.split('\r\n\r\n', 1)
        yield domain, tuple([{url:body}])
        self.increment_counter('commoncrawl', 'processed_pages', 1)

  def combiner(self, key, values):
    yield key, next(values)

  def reducer(self, key, values):
    yield key, next(values)

if __name__ == '__main__':
  SocialSparkRegexJob.run()
