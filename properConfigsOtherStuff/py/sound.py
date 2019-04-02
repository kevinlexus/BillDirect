import urllib.request as ur
import winsound

try:
 s = ur.urlopen("http://127.0.0.1:81003/")
 sl = s.read()
 print(sl)
 winsound.Beep(593, 500)
 winsound.Beep(593, 500)
except:
 winsound.Beep(193, 500)
 winsound.Beep(193, 500)
