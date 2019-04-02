import urllib.request as ur
s = ur.urlopen("http://127.0.0.1:8100/gen?tp=4&klskId=136314&uslId=011&genDt=20.02.2019")
sl = s.read()
print(sl)
