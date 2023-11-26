const zookeeper = require('node-zookeeper-client');
const InfluencerService = require('./services/influencerService');
const ZooKeeperUtils = require('./utils/ZooKeeperUtils');

const zkClient = zookeeper.createClient('localhost:2181'); // Replace with your ZooKeeper server(s) address

const nodesPath = '/nodes'; // Path where nodes register

// Connect to ZooKeeper
zkClient.connect();


zkClient.exists(
    nodesPath,
    (event) => {
        console.log('Got event:', event);
        // Handle event, e.g., node creation or deletion
    },
    (error, stat) => {
        if (error) {
            console.error('Failed to check if parent node exists:', error);
        } else {
            if (!stat) {
                // Parent node doesn't exist, create it
                zkClient.create(
                    nodesPath,
                    Buffer.from('Parent Node Data'), // You can set data for the parent node if needed
                    zookeeper.CreateMode.PERSISTENT,
                    (error, path) => {
                        if (error) {
                            console.error('Failed to create parent node:', error);
                        } else {
                            console.log('Parent node created:', path);
                        }

                        // Close the ZooKeeper connection when done
                        // zkClient.close();
                    }
                );
            } else {
                console.log('Parent node already exists');
                // Close the ZooKeeper connection when done
               // zkClient.close();
            }
        }
    }
);

// Periodically check and distribute ID ranges
setInterval(() => {
    zkClient.getChildren(
        nodesPath,
        (event) => {
            console.log('Got event:', event);
            // Handle event, e.g., a new node registered or existing node removed
        },
         (error, children) => {
            if (error) {
                console.error('Failed to get children:', error);
            } else {
               // console.log("children: ", children)
                const childrenCount = children.length;
                const res =  InfluencerService.geIdRangeOfInfluencers();
                const { startId, endId} = res
                // Calculate and assign ID ranges
                children.forEach((node, index) => {
                    const idRange = ZooKeeperUtils.calculateIdRangeForNode(index, childrenCount, startId, endId);
                    console.log({idRange})
                    const nodePath = `${nodesPath}/${node}`;
                    
                    // Update the znode with the assigned ID range
                    zkClient.setData(
                        nodePath,
                        Buffer.from(`idRange: ${JSON.stringify(idRange)}`),
                        (error, stat) => {
                            if (error) {
                                console.error(`Failed to update znode for ${node}:`, error);
                            } else {
                                console.log(`Assigned ID range for ${node}: ${JSON.stringify(idRange)}`);
                            }
                        }
                    );
                });
            }
        }
    );
}, 10000); // Check every 10 seconds, adjust as needed


// Close the ZooKeeper connection when done
process.on('SIGINT', () => {
    zkClient.close();
    process.exit();
});

