from .legacy_base_action import BaseAction, ActionContext, ActionResult
from typing import Dict, Any, List
import json
from datetime import datetime
import os

class APIInterceptAction(BaseAction):
    def __init__(self):
        super().__init__("api_intercept")
        self._requests = []
        self._responses = []
    
    def _execute(self, context: ActionContext) -> Dict[str, Any]:
        page = context.page
        
        def on_request(request):
            self._requests.append({
                'url': request.url,
                'method': request.method,
                'headers': request.headers,
                'post_data': request.post_data,
                'time': datetime.utcnow().isoformat()
            })
        
        def on_response(response):
            try:
                body = response.body()
                body_str = body.decode('utf-8', errors='ignore')
            except:
                body_str = ''
            
            self._responses.append({
                'url': response.url,
                'status': response.status,
                'headers': response.headers,
                'body': body_str,
                'time': datetime.utcnow().isoformat()
            })
        
        page.on('request', on_request)
        page.on('response', on_response)
        
        trigger_selector = context.config.get('trigger_selector')
        if trigger_selector:
            page.click(trigger_selector)
        
        wait_for = context.config.get('wait_for', 'networkidle')
        page.wait_for_load_state(wait_for)
        
        har_data = self._generate_har()
        har_path = f'/app/artifacts/{context.job.id}/network_trace.har'
        os.makedirs(os.path.dirname(har_path), exist_ok=True)
        
        with open(har_path, 'w') as f:
            json.dump(har_data, f, indent=2)
        
        page.remove_listener('request', on_request)
        page.remove_listener('response', on_response)
        
        return {
            'data': {
                'requests_count': len(self._requests),
                'responses_count': len(self._responses),
                'har_path': har_path
            },
            'artifacts': [har_path]
        }
    
    def _generate_har(self) -> dict:
        entries = []
        
        for req, resp in zip(self._requests, self._responses):
            entry = {
                'startedDateTime': req['time'],
                'time': (datetime.fromisoformat(resp['time']) - datetime.fromisoformat(req['time'])).total_seconds() * 1000,
                'request': {
                    'method': req['method'],
                    'url': req['url'],
                    'httpVersion': 'HTTP/1.1',
                    'headers': [{'name': k, 'value': v} for k, v in req['headers'].items()],
                    'queryString': [],
                    'cookies': [],
                    'headersSize': -1,
                    'bodySize': len(str(req.get('post_data', '')))
                },
                'response': {
                    'status': resp['status'],
                    'statusText': '',
                    'httpVersion': 'HTTP/1.1',
                    'headers': [{'name': k, 'value': v} for k, v in resp['headers'].items()],
                    'content': {
                        'size': len(resp['body']),
                        'mimeType': resp['headers'].get('content-type', ''),
                        'text': resp['body']
                    },
                    'redirectURL': '',
                    'headersSize': -1,
                    'bodySize': len(resp['body']),
                    '_transferSize': -1
                },
                'cache': {},
                'timings': {'send': 0, 'wait': 0, 'receive': 0}
            }
            entries.append(entry)
        
        return {
            'log': {
                'version': '1.2',
                'creator': {'name': 'APIInterceptAction', 'version': '1.0'},
                'pages': [],
                'entries': entries
            }
        }
