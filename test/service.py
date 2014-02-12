import urllib2, urllib, sys

link = sys.argv[1]
config = "name=links;pattern=.*;[links]=a"

post_data = {"link": link, "config": config}
data = urllib.urlencode(post_data)
req = urllib2.Request("http://10.105.75.174:8880/pagemining/extractor", data)
response = urllib2.urlopen(req)
content = response.read()

print content
