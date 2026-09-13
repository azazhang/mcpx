#!/usr/bin/env python3
import json, urllib.request

class MCPClient:
    def __init__(self,url):
        self.url=url; self.session=None; self.n=10
        self.base={'Accept':'application/json, text/event-stream','Content-Type':'application/json'}
    def _parse(self,raw):
        t=raw.decode().strip()
        if t.startswith('event:'):
            rows=[ln[5:].strip() for ln in t.splitlines() if ln.startswith('data:')]
            if not rows: raise RuntimeError('SSE without data')
            return json.loads(rows[-1])
        return json.loads(t)
    def _post(self,body,session=True):
        h=dict(self.base)
        if session and self.session:
            h['Mcp-Protocol-Version']='2025-06-18';h['Mcp-Session-Id']=self.session
        req=urllib.request.Request(self.url,data=json.dumps(body).encode(),headers=h)
        with urllib.request.urlopen(req,timeout=30) as r:
            if not self.session:
                self.session=r.headers.get('Mcp-Session-Id') or r.headers.get('mcp-session-id')
            raw=r.read()
        return self._parse(raw) if raw.strip() else None
    def init(self):
        x=self._post({'jsonrpc':'2.0','id':1,'method':'initialize','params':{'clientInfo':{'name':'mcpx-public-test','version':'1'},'protocolVersion':'2025-06-18','capabilities':{}}},False)
        if not self.session: raise RuntimeError('no session')
        self._post({'jsonrpc':'2.0','method':'notifications/initialized','params':{}})
        return x
    def tools(self):
        self.n+=1;return self._post({'jsonrpc':'2.0','id':self.n,'method':'tools/list','params':{}})['result']['tools']
    def call_raw(self,name,args):
        self.n+=1;return self._post({'jsonrpc':'2.0','id':self.n,'method':'tools/call','params':{'name':name,'arguments':args}})
    def call(self,name,args):
        x=self.call_raw(name,args)
        if 'error' in x:return {'jsonrpc_error':x['error']}
        return x['result']

def state(result):
    return (result.get('structuredContent') or {}).get('state') if isinstance(result,dict) else None

def sc(result):
    return result.get('structuredContent') or {}
