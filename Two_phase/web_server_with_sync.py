__author__ = 'santhoshkumar'
import web
import json
import threading
import time

#urls = ('/(.*)', 'API')
urls = ('/two_phase_commit/(.*)', 'API')
app = web.application(urls, globals())

db = {}
nextid = 0
no_of_mappers = 3
mappers_in_exec = []
abort=False

class API():
    def GET(self, id=None):
        global db, nextid, mappers_in_exec, no_of_mappers
        lock = threading.Lock()
        if id not in mappers_in_exec:
            lock.acquire()
            try:
                mappers_in_exec.append(id)
            finally:
                lock.release()
        while(len(mappers_in_exec) < no_of_mappers):{}
        print "------",id,len(mappers_in_exec)
        return json.dumps("start_transaction")

    def POST(self, id=None):
        global db, nextid, mappers_in_exec, no_of_mappers,abort
        req_data=json.loads(web.data())
        print "*********",req_data['req_status']
        db[id] = req_data['req_status']
        if db[id] == 'abort':
            abort=True
        while( len(db) < no_of_mappers and not abort):{}
            
        print "------",id,len(mappers_in_exec)
        if (len(db) == no_of_mappers)  and not abort:
            return json.dumps({'status': 'commit'})
        else:
            return json.dumps({'status': 'abort'})

if __name__ == "__main__":
    app.run()
