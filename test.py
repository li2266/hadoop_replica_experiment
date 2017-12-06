import time
import threading
import config
from operator import itemgetter, attrgetter

class test():
	def __init__(self, m):
		print("haha")
		self.count = m

	def write(self, name):
		while self.count > 0:
			self.count -= 1
			time.sleep(1)
			print("{} {}".format(name, self.count))


t = test(100)

t1 = threading.Thread(target=t.write, args=("haha", ))
t1.start()
time.sleep(0.5)

t1 = threading.Thread(target=t.write, args=("hehe", ))
t1.start()

'''

class node():
	def __init__(self, name, rackid):
		# node name
		self.node_name = name
		# rackid
		self.rack_id = rackid
		# size of hard disk, unit is MB
		self.hard_drive_size = config.hard_drive_size
		# util of the hard disk
		# range (0, 100), not useful in this program
		self.util = 0
		# the number of block we have on that node
		self.block_amount = 0
		# block size
		self.block_size = config.block_size
		
		# disk speed
		self.disk_speed = config.disk_speed
		# current running tasks
		self.running_tasks = 0

		pass

	# write data to a node
	def write(self):
		self.running_tasks += 1
		rest_size = 64
		sub_speed = self.disk_speed / self.running_tasks
		sub_running_time = rest_size / sub_speed
		while sub_running_time > 0:
			time.sleep(0.1)
			rest_size -= 0.1 * sub_speed
			sub_speed = self.disk_speed / self.running_tasks
			sub_running_time = rest_size / sub_speed
		self.running_tasks -= 1
		self.block_amount += 1
		return 0

	def __repr__(self):
            return repr((self.node_name, self.block_amount))


nodes = list()
nodes.append(node("haha", 1))
nodes[0].block_amount = 25
nodes.append(node("s", 3))
nodes[1].block_amount = 254
nodes.append(node("s", 4))
nodes[2].block_amount = 2
nodes.append(node("tttt", 9))
nodes[3].block_amount = 99

s = sorted(nodes, key=attrgetter('block_amount'))
print(s)
s = sorted(s, key=attrgetter('node_name'), reverse=True)
print(s)



'''





