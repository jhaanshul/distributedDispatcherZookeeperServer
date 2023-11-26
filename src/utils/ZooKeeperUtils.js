// utils/ZooKeeperUtils.js

class ZooKeeperUtils {
    static calculateIdRangeForNode(index, numPartitions, startId, endId) {
        if (startId > endId) {
          throw new Error('Invalid range: startId must be less than or equal to endId');
        }
      
        const totalIds = endId - startId + 1;
        const interval = Math.floor(totalIds / numPartitions);
        const remainder = totalIds % numPartitions;
        const rangeStart = startId + index * interval + Math.min(index, remainder);
        const rangeEnd = rangeStart + interval + (index < remainder ? 1 : 0) - 1;
        return { start: rangeStart, end: rangeEnd };
      }
}

module.exports = ZooKeeperUtils;
