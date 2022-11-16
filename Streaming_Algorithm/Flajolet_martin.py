from blackbox import BlackBox
import random
import sys
import binascii


def generateHashFunc(num_func):
    param_a_list = random.sample(range(1, sys.maxsize - 1), num_func)
    param_b_list = random.sample(range(0, sys.maxsize - 1), num_func)
    return param_a_list, param_b_list

def getMedian(l):
    l = sorted(l)
    if len(l) % 2 == 0:
        return int((l[int(len(l)/2) - 1] + l[int(len(l)/2)]) / 2)
    else:
        return int(l[int(len(l)/2)])

def calculateHashedValue(a, b, m=233333333):
    def hash_func(x):
        return ((a * x + b) % 5330786047) % m
    return hash_func

def getHash(param_a_list, param_b_list):
    hash_value= []
    param_as = param_a_list
    param_bs = param_b_list

    for a, b in zip(param_as, param_bs):
        hash_value.append(calculateHashedValue(a, b))
    return hash_value

def myhashs(s):
    result = []
    hash_function_list = getHash(a_list, b_list)
    for f in hash_function_list:
        result.append(f(int(binascii.hexlify(s.encode('utf8')), 16)))
    return result


# file_name = 'users.txt'
# stream_size = 300
# num_to_ask = 30  
# output_file = 'task2.csv'

file_name = sys.argv[1]
stream_size = int(sys.argv[2])
num_to_ask = int(sys.argv[3]) 
output_file = sys.argv[4]

bx = BlackBox()
num_func = 50
a_list, b_list = generateHashFunc(num_func)
output = []

for i in range(num_to_ask):
    stream_users = bx.ask(file_name, stream_size)
    ground_truth = len(set(stream_users))
    longest_trailing_zeros = [0]*(len(a_list)+len(b_list))
    for user in stream_users:
        hash_v = myhashs(user)
        len_zero = []
        for v in hash_v:
            bin_hashed = str(bin(v)[2:])
            len_zero.append(len(bin_hashed)-len(bin_hashed.rstrip('0')))
        longest_trailing_zeros = [max(longest_trailing_zeros[u], len_zero[u]) for u in range(len(len_zero))]
    window_estimate = int(getMedian(2**k for k in longest_trailing_zeros))
    output.extend((i,ground_truth,window_estimate))

with open(output_file,'w+') as f:
    f.write('Time,Ground Truth,Estimation\n')
    for i in range(0,len(output),3):
        f.write(str(output[i])+','+str(output[i+1])+','+str(output[i+2]))
        f.write('\n')


        
