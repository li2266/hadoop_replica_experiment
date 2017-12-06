import schedulars
import config
import time

class executor:
	def __init__(self):
		pass
	def write(self, file_size):
		
	

# only writing use multi thread, class node there is just a config/data of a node
class node:
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



