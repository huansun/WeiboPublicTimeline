import datetime
import time
import threading
from weibo import APIError

class Scheduler:
    def __init__(self):
        self.clients_ = []

    def add_client(self, client):
        self.clients_.append(client)
    
    def get_next(self):
        min_scheduled_time = None
        min_scheduled_client = None
        for client in self.clients_:
            if (not min_scheduled_client or client.get_scheduled_time() < min_scheduled_time):
                min_scheduled_time = client.get_scheduled_time()
                min_scheduled_client = client
        return min_scheduled_client
                
    def get_next_runable(self):
        client = self.get_next()
        to_sleep = (client.get_scheduled_time() - datetime.datetime.now()).total_seconds()
        print 'sleep:' + str(to_sleep)
        if to_sleep > 0:
            time.sleep(to_sleep)
        return client 
        
class SchedulerClient:
    def __init__(self, client):
        self.client_ = client
        self.scheduled_time_ = None
        self.reest_time_ = None
        self.remain_ = None
        self.update_rate_limit()
    
    def update_rate_limit(self):
        result = self.client_.account.rate_limit_status.get()
        print 'update_rate_limit:' + str(result)
        self.remain_ = result['remaining_user_hits']
        self.reset_time_ = datetime.datetime.strptime(result['reset_time'], '%Y-%m-%d %H:%M:%S') - datetime.timedelta(hours=15)
        print self.reset_time_
 
    def update_scheduled_time(self):
        now = datetime.datetime.now()
        if now > self.reset_time_:
            self.update_rate_limit()
        if self.remain_ > 0:
            self.scheduled_time_ = now + (self.reset_time_ - now) / self.remain_
        else:
            self.scheduled_time_ = self.reset_time_ + datetime.timedelta(seconds = 1)
        
    def get_scheduled_time(self):
        if not self.scheduled_time_:
            self.update_scheduled_time()
        return self.scheduled_time_
    
    def get_client(self):
        return self.client_
    
    def invoke(self):
        self.remain_ -= 1
        self.update_scheduled_time()
        
class ThreadClient(threading.Thread):
    def __init__(self, scheduler_client, output):
        threading.Thread.__init__(self)
        self.scheduler_client_ = scheduler_client
        self.output_ = output
        
    def run(self):
        while True:
            to_sleep = (self.scheduler_client_.get_scheduled_time() - datetime.datetime.now()).total_seconds()
            print to_sleep
            if to_sleep > 0:
                time.sleep(to_sleep)
            try:
                statuses = self.scheduler_client_.get_client().statuses.public_timeline.get(count = 200)['statuses']
                if not statuses:
                    print 'no result'
                self.output_.write(statuses)
            except APIError as e:
                print e
#            except:
#                print 'error'
            self.scheduler_client_.invoke()
 
