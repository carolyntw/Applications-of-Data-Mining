from pyspark import SparkContext, SparkConf
from blackbox import BlackBox
import binascii
import random
import sys
import time

def myhashs(s):
    result = []
    hash_function_list = bloomFilter(a_list, b_list)
    for f in hash_function_list:
        result.append(f(int(binascii.hexlify(s.encode('utf8')), 16)))
    return result

def generateHashFunc(num_func):
    param_a_list = random.sample(range(1, sys.maxsize - 1), num_func)
    param_b_list = random.sample(range(0, sys.maxsize - 1), num_func)
    
    return param_a_list, param_b_list

def calculateHashedValue(a, b, m=69997):
    def hash_func(x):
        return ((a * x + b) % 25165843) % m
    return hash_func

def bloomFilter(param_a_list, param_b_list):
    bloom_filter = []
    param_as = param_a_list
    param_bs = param_b_list

    for a, b in zip(param_as, param_bs):
        bloom_filter.append(calculateHashedValue(a, b))
    return bloom_filter


startTime = time.time()

conf = SparkConf().setMaster("local[*]").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
sc = SparkContext(conf=conf)

file_name = sys.argv[1]
stream_size = int(sys.argv[2])
num_to_ask = int(sys.argv[3]) 
output_file = sys.argv[4]

# file_name = 'users.txt'
# stream_size = 100
# num_to_ask = 30 
# output_file = 'task1.csv'

bx = BlackBox()

global_hash_array = [-1] * 69997
num_func = 3
a_list, b_list = generateHashFunc(num_func)


output = []
shown = set()

for i in range(num_to_ask):
    stream_users = bx.ask(file_name,stream_size)
    hash_showed = []
    for u in stream_users:
        hash_v = myhashs(u)
        count = 0
        for value in hash_v:
            count += global_hash_array[value]
        if count == len(hash_v):
            hash_showed.append(u)

    true_hashed_show = []
    for k in stream_users:
        if k in shown:
            true_hashed_show.append(k)
    if len(hash_showed) == 0:
        output.extend((i,0.0))
    else:
        hash_not_showed =[]
        for n in hash_showed:
            if n not in true_hashed_show:
                hash_not_showed.append(n) 
        output.extend((i,len(hash_not_showed)/len(hash_showed)))
    
    for u in stream_users:
        shown.add(u)
        hash_v = myhashs(u)
        for v in hash_v:
            global_hash_array[v] = 1

# print(output)

with open(output_file,'w+') as f:
    f.write('Time,FPR\n')
    for i in range(0,len(output),2):
        f.write(str(output[i])+','+str(output[i+1]))
        f.write('\n')