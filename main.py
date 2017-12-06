import node

c = node.cluster(50, 1)
c.write(10000)
