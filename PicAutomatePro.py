from confluent_kafka import Producer
import base64
import os
#import cv2
p=Producer({'bootstrap.servers':'130.127.133.89:9092', 'queue.buffering.max.messages':1000000, 'batch.num.messages':50})
#p = Producer({'bootstrap.servers': 'node0087:9092'})
list=os.listdir('/home/sid/Documents/EP-01-07728_0016/')
number_files=len(list)
print(number_files)
num=1


dir = '/home/sid/Documents/EP-01-07728_0016/'
for filename in sorted(os.listdir('/home/sid/Documents/EP-01-07728_0016/')):
#while(num<100)
	i=0
	j=0	
	num_str=str(num)
	print(num_str)
	g=filename[:17]+num_str+'.JPG'
	print(g)
	print(filename)
	g=dir+g
	filename=dir+filename
	os.rename(filename,g)
	print(filename)	
	
	topic='topic'+num_str
	image=open(g,'rb')
	size=os.path.getsize(g)
	print(size)
	size10kb=size/10240

	while j<size10kb:
		image.seek(i,0)
		image_read=image.read(10240)
		image_64_encode=base64.encodestring(image_read)
		p.produce(topic, image_64_encode)
		i+=10240
		j+=1
	

	num=num+1
p.flush()

'''image=open('EP-01-07728_0016_'+num+'.JPG','rb')
#success, image=f.read()
#print(success)
topic='topic'+num
i=0
j=0
size=os.path.getsize('/home/sid/Documents/EP-01-07728_0016_'+num+'.JPG')
print(size)
size10kb=size/10240
#img=cv2.imread('Pic.png',0)
#image = open('EP-01-07728_0016_0239.JPG', 'rb')
while j<size10kb:
	image.seek(i,0)
	image_read=image.read(10240)
	image_64_encode=base64.encodestring(image_read)
	p.produce(topic, image_64_encode)
	i+=10240
	j+=1


#with open("kafka_2.11-1.0.0/Pic.png","rb") as imageFile:
#	str = base64.b64encode(imageFile.read())
#	p.produce('test',str)
#f=open("sampleinput", "r")
#for data in f.read():
#    print(data)
#    p.produce('mytopic', data.encode('utf-8'))
#for data in imageFile.read():
#    p.produce('test', bytearray(imageFile.read()))
p.flush()'''
