import urllib2

response = urllib2.urlopen('http://127.0.0.1:8100/gen?tp=2&houseId=4476&genDt=20.02.2019')
print response.info()
html = response.read()
response.close()
