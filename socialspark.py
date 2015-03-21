import re
from urlparse import urlparse
from collections import Counter
from mrcc import CCJob
TWITTER_PATTERN = re.compile("twitter\.com\/[a-zA-Z0-9_]{1,15}\/?\Z")
YOUTUBE_PATTERN = re.compile("youtube\.com\/user\/[a-zA-Z0-9]{1,20}\/?\Z")
INSTAGRAM_PATTERN = re.compile("instagram\.com\/[a-zA-Z0-9_.]{1,30}\/?\Z")

class SocialSparkRegexJob(CCJob):
  def process_record(self, record):
    if record['Content-Type'] == 'application/http; msgtype=response':
      url = record["WARC-Target-URI"]
      payload = record.payload.read()
      headers, body = payload.split('\r\n\r\n', 1)
      domain = urlparse(url).netloc

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
        print url
        yield domain, tuple([{url:body}])
        self.increment_counter('commoncrawl', 'processed_pages', 1)

  def combiner(self, key, values):
    #yield key, next(values)
    #yield key, values
    for val in values:
      yield key, val

  def reducer(self, key, values):
    #yield key, next(values)
    for val in values:
      yield key, val

if __name__ == '__main__':
  SocialSparkRegexJob.run()
