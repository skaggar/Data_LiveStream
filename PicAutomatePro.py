from confluent_kafka import Producer
import base64
import os
import shutil
#import cv2

p=Producer({'bootstrap.servers':'130.127.55.239:9092', 'queue.buffering.max.messages':1000000, 'batch.num.messages':50})
#p1=Producer({'bootstrap.servers':'130.127.55.239:9092', 'queue.buffering.max.messages':1000000, 'batch.num.messages':50})

dir = '/home/sid/Documents/EP-01-07728_0016/'
#p = Producer({'bootstrap.servers': 'node0087:9092'})
list=os.listdir('/home/sid/Documents/EP-01-07728_0016/')
number_files=len(list)
print(number_files)
num=1
number_files_str=str(number_files)


#Writing number of files, the folder name and name of the files into a Text file

f=open("/home/sid/Documents/kafka_files/file_meta.txt","w")
f.close()
f=open("/home/sid/Documents/kafka_files/file_meta.txt","a")
f.write(number_files_str)
f.write("\n")


'''
f.write("EP-01-07728_0016\n")
for files in sorted(os.listdir(dir)):
	f.write(files)
	f.write("\n")
f.close()
'''


f1=open("/home/sid/Documents/kafka_files/file_meta.txt", "r")
for data in f1.read():
	p.produce('meta', data.encode('utf-8'))
p.flush()


#shutil.copytree('/home/sid/Documents/EP-01-07728_0016/','/home/sid/Documents/EP-01-07728_0017/')

subdir='EP-01-07728_0016_'
#Iteration to rename the file and transfer it
for filename in sorted(os.listdir('/home/sid/Documents/EP-01-07728_0016/')):
	i=0
	j=0	
	num_str=str(num)
	print(num_str)

	# Changing the File Name
	g=filename[:17]+num_str+'.JPG'
	print(filename)
	g=dir+g
	filename=dir+filename
	os.rename(filename,g)
	print(g)	

	# Kafka Topic Name for the image
	topic=subdir+'0000'+num_str
	#topic=subdir
	
	#Opening the file
	image=open(g,'rb')

	#Fetching the size for the file
	size=os.path.getsize(g)
	print(size)

	# Splitting the file into 1 Mb clusters
	size10kb=size/10240

	#Encode and transfer the clusters
	while j<size10kb:
		image.seek(i,0)
		image_read=image.read(10240)
		image_64_encode=base64.encodestring(image_read)
		p.produce(topic, image_64_encode)
		i+=10240
		j+=1
	num=num+1

#Flush the files to Kafka Broker
	p.flush()







'''
image=open('EP-01-07728_0016_'+num+'.JPG','rb')
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