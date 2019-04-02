import winsound
import urllib.request as ur
try:
 s = ur.urlopen("http://127.0.0.1:8100/gen?tp=2&genDt=20.02.2019")
 sl = s.read()
 print(sl)
 winsound.Beep(593, 500)
 winsound.Beep(593, 500)
except:
 winsound.Beep(193, 500)
 winsound.Beep(193, 500)

try:
 s = ur.urlopen("http://127.0.0.1:8100/gen?tp=0&genDt=20.02.2019")
 sl = s.read()
 print(sl)
 winsound.Beep(593, 500)
 winsound.Beep(593, 500)
except:
 winsound.Beep(193, 500)
 winsound.Beep(193, 500)
