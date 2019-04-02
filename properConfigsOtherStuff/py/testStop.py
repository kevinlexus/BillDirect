import urllib2

response = urllib2.urlopen('http://127.0.0.1:8100/gen?stop=1')
print response.info()
html = response.read()
response.close()
