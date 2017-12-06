import time
import threading

class config():
	def __init__(self, count):
		self.count = count

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

t2 = threading.Thread(target=t.write, args=("hehe", ))
t2.start()