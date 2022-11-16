from blackbox import BlackBox
import random
import sys


file_name = 'users.txt'
stream_size = 100
num_to_ask = 30  
output_file = 'task3.csv'


bx = BlackBox()
random.seed(553)
stream_users_init = bx.ask(file_name, stream_size)
output = []
output.extend([100,stream_users_init[0],stream_users_init[20],stream_users_init[40],stream_users_init[60],stream_users_init[80]])
l = len(stream_users_init)
n = l
for i in range(2,num_to_ask+1):
    stream_users = bx.ask(file_name, stream_size)
    for u in stream_users:
        n += 1
        num = random.random()
        if num < l/n:
            replace = random.randint(0,l-1)
            stream_users_init[replace] = u
    output.extend([100*(i),stream_users_init[0],stream_users_init[20],stream_users_init[40],stream_users_init[60],stream_users_init[80]])

with open(output_file,'w+') as f:
    f.write('seqnum,0_id,20_id,40_id,60_id,80_id\n')
    for i in range(0,len(output),6):
        f.write(str(output[i])+','+str(output[i+1])+','+str(output[i+2])+','+str(output[i+3])+','+str(output[i+4])+','+str(output[i+5]))
        f.write('\n')
