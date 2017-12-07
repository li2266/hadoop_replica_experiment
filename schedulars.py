from operator import itemgetter, attrgetter
import config
from random import randint

class origin_schedular():

	def __init__(self):
		pass

	def replica_node(self, node_list, rack_number):
		res = list()
		if config.replica_number < 1:
			print("Wrong replica number")
			return None

		first_rack = randint(0, rack_number - 1)
		tmp_list = [element for element in node_list if element.rack_id == first_rack]
		first_node = randint(0, len(tmp_list) - 1)
		res.append(tmp_list[first_node])
		if config.replica_number == 1:
			return res

		second_rack = first_rack
		while second_rack == first_rack and rack_number > 1:
			second_rack = randint(0, rack_number - 1)
		tmp_list = [element for element in node_list if element.rack_id == second_rack]
		second_node = randint(0, len(tmp_list) - 1)
		res.append(tmp_list[second_node])
		if config.replica_number == 2:
			return res

		third_rack = first_rack
		tmp_list = [element for element in node_list if element.rack_id == third_rack]
		third_node = randint(0, len(tmp_list) - 1)
		res.append(tmp_list[third_node])
		if config.replica_number == 3:
			return res

		count = 3
		while config.replica_number - count > 0:
			rand_index = (0, len(node_list) - 1)
			res.append(node_list[rand_index])
			count += 1
		return res
		pass

class smart_schedular():

	def __init__(self):
		pass

	def replica_node(self, node_list, rack_number):
		if config.replica_number < 1:
			print("Wrong replica number")
			return None
		tmp_list = sorted(node_list, key=attrgetter('running_tasks'))
		tmp_list = sorted(tmp_list, key=attrgetter('block_amount'))
		return tmp_list[0 : config.replica_number]

