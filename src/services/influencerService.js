// services/ZooKeeperService.js
const zookeeper = require('node-zookeeper-client');

class InfluencerService {
   static geIdRangeOfInfluencers(){
    // connect to influencerModel that fetches this range from Database
     return {startId: 1000000, endId: 1999999}
   }
}

module.exports = InfluencerService;
