import node

c = node.cluster(50, 2)
c.write(10000)
