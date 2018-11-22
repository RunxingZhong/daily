```
https://blog.csdn.net/cymy001/article/details/78483723

===============================================
init sc
===============================================
1# init 
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('split_data')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

2# init local
from pyspark import SparkContext, SparkConf
import os
os.environ['SPARK_HOME'] = '/home/zhongrunxing/spark/'
os.environ['JAVA_HOME'] = '/home/zhongrunxing/java/jdk1.8.0_171/'

sc = SparkContext("local", 'test')
rdd = sc.textFile(path + 'train_10000.gc')

# sc = SparkContext.getOrCreate()


===============================================
init rdd
===============================================
3# 
rdd = sc.parallelize([1,2,3,4,5])

4#
getNumPartitions()方法查看list被分成了几部分
rdd.getNumPartitions() 

5#
glom().collect()查看分区状况
rdd.glom().collect()

6#
first()方法取读入的rdd数据第一个item
rdd.first()

===============================================
trainsform a single rdd, item is a value, v
===============================================
7## RDD Transformation, item is a simple value
map() 对RDD的每一个item都执行同一个操作
flatMap() 对RDD中的item执行同一个操作以后得到一个list，然后以平铺的方式把这些list里所有的结果组成新的list
filter() 筛选出来满足条件的item
distinct() 对RDD中的item去重
sample() 从RDD中的item中采样一部分出来，有放回或者无放回
sortBy() 对RDD中的item进行排序
reduce(lambda x, y: x + y)


===============================================
trainsform a single rdd, item is a pair, (k, v)
===============================================
8# RDD Transformation, item is a pair
当遇到更复杂的结构，比如被称作“pair RDDs”的以元组形式组织的k-v对（key, value），Spark中针对这种item结构的数据，定义了一些transform和action:

reduceByKey(): 对所有有着相同key的items执行reduce操作
groupByKey(): 返回类似(key, listOfValues)元组的RDD，后面的value List 是同一个key下面的
sortByKey(): 按照key排序
countByKey(): 按照key去对item个数进行统计
collectAsMap(): 和collect有些类似，但是返回的是k-v的字典

rdd.map(lambda x: x[0]) # get key
rdd.map(lambda x: x[1]) # get value


9# word count
rdd=sc.parallelize(["Hello hello", "Hello New York", "York says hello"])

rdd\
.flatMap(lambda x: x.split(' '))\
.map(lambda x: x.lower())\
.map(lambda x: (x, 1))\
.reduceByKey(lambda x, y: x + y)\
.collect()


===============================================
interaction betweeen 2 rdd
===============================================
10# RDD间的操作
（1）如果有2个RDD，可以通过下面这些操作，对它们进行集合运算得到1个新的RDD
rdd1.union(rdd2): 所有rdd1和rdd2中的item组合（并集）
rdd1.intersection(rdd2): rdd1 和 rdd2的交集
rdd1.substract(rdd2): 所有在rdd1中但不在rdd2中的item（差集）
rdd1.cartesian(rdd2): rdd1 和 rdd2中所有的元素笛卡尔乘积（正交和）

2）在给定2个RDD后，可以通过一个类似SQL的方式去join它们
join
leftOuterJoin
rightOuterJoin


# Home of different people
homesRDD = sc.parallelize([
        ('Brussels', 'John'),
        ('Brussels', 'Jack'),
        ('Leuven', 'Jane'),
        ('Antwerp', 'Jill'),
    ])

# Quality of life index for various cities
lifeQualityRDD = sc.parallelize([
        ('Brussels', 10),
        ('Antwerp', 7),
        ('RestOfFlanders', 5),
    ])
rdd2 = homesRDD.join(lifeQualityRDD)   #join
rdd2.collect()
[('Antwerp', ('Jill', 7)),
 ('Brussels', ('John', 10)),
 ('Brussels', ('Jack', 10))]

rdd2.map(lambda x: x[0]) # get key
rdd2.map(lambda x: x[1]) # get value


===============================================
check rdd
===============================================
11#
惰性计算，actions方法
特别注意：Spark的一个核心概念是惰性计算。当你把一个RDD转换成另一个的时候，这个转换不会立即生效执行！！！Spark会把它先记在心里，等到真的有actions需要取转换结果时，才会重新组织transformations(因为可能有一连串的变换)。这样可以避免不必要的中间结果存储和通信。

常见的action如下，当它们出现的时候，表明需要执行上面定义过的transform了:

collect(): 计算所有的items并返回所有的结果到driver端，接着 collect()会以Python list的形式返回结果
first(): 和上面是类似的，不过只返回第1个item
take(n): 类似，但是返回n个item
count(): 计算RDD中item的个数
top(n): 返回头n个items，按照自然结果排序
reduce(): 对RDD中的items做聚合

12# cache()
有时候需要重复用到某个transform序列得到的RDD结果。但是一遍遍重复计算显然是要开销的，所以我们可以通过一个叫做cache()的操作把它暂时地存储在内存中。缓存RDD结果对于重复迭代的操作非常有用，比如很多机器学习的算法，训练过程需要重复迭代。
numbersRDD = sc.parallelize(np.linspace(1.0, 10.0, 10))
squaresRDD = numbersRDD.map(lambda x: x**2)

squaresRDD.cache()  # Preserve the actual items of this RDD in memory

avg = squaresRDD.reduce(lambda x, y: x + y) / squaresRDD.count()
print(avg)


13#
