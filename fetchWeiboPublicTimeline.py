import csv
import codecs
import datetime
import time
import locale

from weibo import APIClient
from weibo import APIError
from scheduler import SchedulerClient
from scheduler import Scheduler
from scheduler import ThreadClient
import threading



def seralize(s):
    header = ['user.bi_followers_count', 'user.domain', 'user.avatar_large', 'user.statuses_count', 'user.allow_all_comment', 'user.id', 'user.city', 'user.province', 'user.follow_me', 'user.verified_reason', 'user.followers_count', 'user.location', 'user.mbtype', 'user.profile_url', 'user.block_word', 'user.star', 'user.description', 'user.friends_count', 'user.online_status', 'user.mbrank', 'user.idstr', 'user.profile_image_url', 'user.allow_all_act_msg', 'user.verified', 'user.geo_enabled', 'user.screen_name', 'user.lang', 'user.weihao', 'user.remark', 'user.favourites_count', 'user.name', 'user.url', 'user.gender', 'user.created_at', 'user.verified_type', 'user.following', 'reposts_count', 'favorited', 'truncated', 'text', 'created_at', 'mlevel', 'idstr', 'mid', 'visible', 'attitudes_count', 'in_reply_to_screen_name', 'source', 'in_reply_to_status_id', 'comments_count', 'geo', 'id', 'in_reply_to_user_id']
    result_map = {}
    for k, v in s.iteritems():
        if k == 'user':
            for k1, v1 in v.iteritems():
                result_map['user.' + k1] = v1
        else:
            result_map[k] = v
    result_list = []
    for h in header:
        if h in result_map:
            s = result_map[h]
            result_list.append(s.encode('utf-8') if isinstance(s, unicode) else s)
        else:
            result_list.append('')
    return result_list
                
# to see how to get the following http://www.cs.cmu.edu/~lingwang/weiboguide/
APP_KEY = '' # app key
APP_SECRET = '' # app secret
CALLBACK_URL = '' # callback url

scheduler = Scheduler()
# tokens used for multi threading
tokens = ['',
          '',
          '',
          '',
          '',
          '',
          '']


class ThreadSafeWriter:
    def __init__(self, fout, csv_writer):
        self.fout_ = fout
        self.csv_writer_ = csv_writer
        self.lock_ = threading.Lock()
        self.output_size_ = 0

    def write(self, statuses):
        self.lock_.acquire()
        for s in statuses:
            serialized = seralize(s)
            self.csv_writer_.writerow(serialized)
        self.output_size_ += 1
        self.fout_.flush()
        self.lock_.release()

    def output_size(self):
        return self.output_size_

fout = open('weibo.csv','a+')     
csv_writer = csv.writer(fout)
output = ThreadSafeWriter(fout, csv_writer)
for token in tokens:
    client = APIClient(app_key=APP_KEY, app_secret=APP_SECRET, redirect_uri=CALLBACK_URL)
    client.set_access_token(token, 0)
    tclient = ThreadClient(SchedulerClient(client), output)
    tclient.start()

start_time = None
start_count = 0
while True:
    if start_time:
        print 'QPH: ' + str(3600 * (output.output_size() - start_count) / (datetime.datetime.now() - start_time).total_seconds())
    start_time = datetime.datetime.now()
    start_count = output.output_size()
    print output.output_size()
    time.sleep(30)
        
fout.close()
