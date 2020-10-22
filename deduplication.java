def deduplicate(stream: DStream[Map[String, String]]): DStream[Map[String, String]] = {
 stream.map(CacheDeduplicator.createPair).transform((rdd, time) => {
	 rdd.repartitionAndSortWithinPartitions(partitioner).mapPartitions(iter => {
		 val batchTime = time.milliseconds
	 val tmpCacheDir = getTempCacheDir(batchTime)
	 val finalCacheDir = getFinalCacheDir(Config.RootDir)

	 val cacheLoader = new CacheLoader(finalCacheDir, tmpCacheDir, batchTime)

	 val input = iter.toVector
	 val output = input.filter {
		 case (partitionKey, fields) =&gt;
		 val eventId = fields(FlowField.EventId)

		 val cache = cacheLoader.getCache(partitionKey)

		 cache.add(eventId)
	 }.map(_._2)

	 cacheLoader.persist()
	 output.iterator
 })

})