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
        if id not in mappers_in_exec:
            mappers_in_exec.append(id)
        if len(mappers_in_exec) < no_of_mappers:
            time.sleep(30)

        print "------",id,len(mappers_in_exec)
        if(len(mappers_in_exec) == no_of_mappers):
            return json.dumps("start_transaction")
        else:
            return json.dumps("do_not_start")

    def POST(self, id=None):
        global db, nextid, mappers_in_exec, no_of_mappers,abort
        req_data=json.loads(web.data())
        print "*********",req_data['req_status']
        db[id] = req_data['req_status']
        if db[id] == 'abort':
            abort=True
        if len(db) < no_of_mappers and not abort:
            time.sleep(30)

        print "------",id,len(mappers_in_exec)
        if (len(db) == no_of_mappers)  and not abort:
            return json.dumps({'status': 'commit'})
        else:
            return json.dumps({'status': 'abort'})

if __name__ == "__main__":
    app.run()
