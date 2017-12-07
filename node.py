import schedulars
import config
import time
import schedulars
import threading

class cluster():
	def __init__(self, node_amount, schedular_type):
		self.nodes = list()
		for i in range(1, node_amount + 1):
			self.nodes.append(node("node" + str(i), i / config.rack_capicity))
			print("Init {} on rack {}".format(self.nodes[i - 1], i % 10))
		self.rack_number = node_amount / config.rack_capicity
		if node_amount % config.rack_capicity != 0:
			self.rack_number += 1

		if schedular_type == 1:
			self.schedular = schedulars.origin_schedular()
		elif schedular_type == 2:
			self.schedular = schedulars.smart_schedular()
		else:
			print("Wrong schedular type")

		self.start_time = time.time()

		t = threading.Thread(target=self.watcher)
		t.start()

	def write(self, file_size):
		block_amount = file_size / config.block_size
		if file_size % config.block_size != 0:
			block_amount += 1
		print("I am cluster and I am trying to write {} to HDFS".format(file_size))
		for i in range(0, block_amount):
			nodes = self.schedular.replica_node(self.nodes, self.rack_number)
			print("replica nodes are {}".format(nodes))
			for node in nodes:
				t = threading.Thread(target=node.write, args=(self.start_time, ))
				t.start()
	def watcher(self):
		while True:
			count = 0
			for node in self.nodes:
				if node.running_tasks != 0:
					count += 1;
			if count == 0:
				print("No node working right now. Used time: {}".format(time.time() - self.start_time))
			else:
				print("{} nodes are BUSY right now".format(count))
			time.sleep(1)


# only writing use multi thread, class node there is just a config/data of a node
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
		# lock for soem variable in the node such as running tasks.
		self.mutex = threading.Lock()

		pass

	# write data to a node
	def write(self, start_time):
		# 10 second to time out
		self.mutex.acquire(10)
		self.running_tasks += 1
		self.mutex.release()
		rest_size = 64
		sub_speed = float(self.disk_speed) / float(self.running_tasks)
		sub_running_time = float(rest_size) / float(sub_speed)
		while sub_running_time > 0:
			time.sleep(0.01)
			rest_size -= 0.01 * sub_speed
			sub_speed = float(self.disk_speed) / float(self.running_tasks)
			sub_running_time = float(rest_size) / float(sub_speed)
			#print("I am {}, I have {} jobs right now and I need {} s to finish CURRENT job".format(self.node_name, self.running_tasks, sub_running_time))
		self.mutex.acquire(10)
		self.running_tasks -= 1
		self.block_amount += 1
		self.mutex.release()
		print("{} done using {}".format(self.node_name, time.time() - start_time))
		return 0

	def __repr__(self):
		return repr((self.node_name, self.block_amount, self.running_tasks))



