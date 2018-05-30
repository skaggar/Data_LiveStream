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
subdir='EP-01-07728_0016_'

dir= '/home/sid/Documents/files/'

fh=open("/home/sid/Documents/NewFile1.txt","w")
fh.write("")
fh.close()
print("Running now")
while running:
    msg = c.poll()
    g=msg.value().decode('utf-8')
    fh=open("/home/sid/Documents/NewFile1.txt","a")
    fh.write(g)
    if(g==''):
        break
    #timer=time.time()+2
    print('Received message: %s' % g)
    #print(time.time())
    #print(timer)
    
        #c.commit()
    fh.close()



fh=open("/home/sid/Documents/NewFile1.txt","r")
count=fh.read()
count=239

print(count)
c.unsubscribe()
while (num<count):
    num=num+1
    num_str=str(num)
    c.subscribe([subdir+'0000'+num_str])
    #topics.append(subdir+num_str)
    print(num)
    print(running)
    r=0
    #d.subscribe([subdir])
    while running:
        r=r+1
        filename=dir+num_str+'.JPG'
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
#        sys.stderr.write('%% %s [%d] reached end at offset %d\n'%
#            (msg.topic(),msg.partition(), msg.offset(), str(msg.key())))
        print(image_64_decode)
    c.unsubscribe()
c.close()


'''filenum=1
i=0
c.subscribe(topics)
while running:
    num_str=str(num)
    #c.subscribe(topics)
    filename=dir+num_str+'.jpg'
    i=i+1
    msg=c.poll()
    g=msg.value()
    image_64_decode=base64.decodestring(g)
    image_result=open(filename, 'ab')
    print("Writing "+filename)
    print(num)
    print(i)
    image_result.write(image_64_decode)
    image_result.close()
    #c.close()
    num=num+1   
    i=0
c.close()

'''


'''
#fh=open("NewFile1","a")
#fh.write("hello")
#while running:
#    msg = c.poll()
        if not msg.error():
        fh=open("NewFile1","a")
        g=msg.value().decode('utf-8')
        fh.write(g)
        print('Received message: %s' % g)
        #c.commit()
        fh.close()
        #fh.write(msg.value().decode('base64'))
    elif msg.error().code() != KafkaError._PARTITION_EOF:
        print(msg.error())
        running = False'''
    #fh=open("NewFile1.jpg","rb")
'''    g=msg.value()
    image_64_decode=base64.decodestring(g)
    image_result=open('test/TestImage2.jpg', 'ab')

    print("Writing")
    image_result.write(image_64_decode)
    image_result.close()
'''#fh.close()
'''c.close()'''
