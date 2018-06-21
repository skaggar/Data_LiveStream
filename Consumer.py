from confluent_kafka import Consumer, KafkaError
import base64
import os
import time
import sys

c = Consumer({'bootstrap.servers': '130.127.55.239:9092', 'group.id': 'mygroup',
              'default.topic.config': {'auto.offset.reset': 'smallest'}, 'fetch.message.max.bytes':150000000})
c.subscribe(['meta'])
#d = Consumer({'bootstrap.servers': '130.127.133.133:9092', 'group.id': 'mygroup',
#             'default.topic.config': {'auto.offset.reset': 'smallest'}, 'fetch.message.max.bytes':150000000})
#d.subscribe(['num'])

running = True
num=0
topics=[]
#number_files=239
#subdir='EP-01-07728_0016_'

dirt= '/home/sid/Documents/files/'
'''
fh=open("/home/sid/Documents/NewFile1.txt","w")
fh.write("")
fh.close()
print("Running now")
while running:
    msg = c.poll()
    g=msg.value().decode('utf-8')
    if(g==''):
        break
    fh=open("/home/sid/Documents/NewFile1.txt","a")
    fh.write(g)

    print('Received message: %s' % g)

    fh.close()

c.unsubscribe()'''

fh=open("/home/sid/Documents/NewFile1.txt","r")
count=fh.readline()
count=count.rstrip()
count=int(count)
print(count)

folder=fh.readline()
folder=folder.rstrip()
print(folder)
if not (os.path.exists(dirt+folder)):
    os.makedirs(dirt+folder)
while (num<count):
    num=num+1
    image=fh.readline()
    image=image.rstrip()
    c.subscribe([image])
    print(num)
    print('running')
    r=0
    #d.subscribe([subdir])
    while running:
        r=r+1
        filename=dirt+folder+'/'+image
        msg=c.poll()
        g=msg.value()
        image_64_decode=base64.decodestring(g)
        if(image_64_decode==b''):
            break
        image_result=open(filename, 'ab')
        print("Writing")
        print(num)
        print(r)
        image_result.write(image_64_decode)
        image_result.close()
        print(image_64_decode)
    c.unsubscribe()
c.close()


