import json, pprint, requests, textwrap

host = 'http://localhost:8998'
headers = {'Content-Type': 'application/json'}

data = {'kind': 'pyspark', 
	"name": "Livy Pi Example", 
	"executorCores":1, 
	"executorMemory":"512m", 
	"driverCores":1, 
	"driverMemory":"512m"}
url = host + '/sessions'

# r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
# pprint.pprint(r.json)
# pprint.pprint(r.headers['Location'])


session_url = host + '/sessions/3'
# r = requests.get(session_url, headers=headers)
# pprint.pprint(r.json())

statements_url = session_url + '/statements'
data = {
  'code': textwrap.dedent("""
    import random
    NUM_SAMPLES = 100000
    def sample(p):
      x, y = random.random(), random.random()
      return 1 if x*x + y*y < 1 else 0

    count = sc.parallelize(xrange(0, NUM_SAMPLES)).map(sample).reduce(lambda a, b: a + b)
    print "Pi is roughly %f" % (4.0 * count / NUM_SAMPLES)
    """)
}
r = requests.post(statements_url, data=json.dumps(data), headers=headers)
pprint.pprint(r.json())

# statement_url = host + '/sessions/0/statements/0'
# r = requests.get(statement_url, headers=headers)
# pprint.pprint(r.json())
# pprint.pprint(r.headers)

# session_url = 'http://localhost:8998/sessions/0'
# r = requests.delete(session_url, headers=headers)
# print(r.json())

# log_url = session_url + '/logs'
# r = requests.get(log_url, headers=headers)
# print(r.json)

