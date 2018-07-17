accum = sc.accumulator(0)
accum
sc.parallelize([1, 2, 3, 4]).foreach(lambda x: accum.add(x))
accum.value