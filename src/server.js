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

function updateChildrenData(client, nodePath, idRange) {
    client.setData(
        nodePath,
        Buffer.from(`idRange: ${JSON.stringify(idRange)}`),
        (error, stat) => {
            if (error) {
                console.error(`Failed to update znode for ${nodePath}:`, error);
            } else {
                console.log(`Assigned ID range for ${nodePath}: ${JSON.stringify(idRange)}`);
            }
        }
    );
}

function distributedIdsAmongChildren(children) {
    const childrenCount = children.length;
    const res = InfluencerService.geIdRangeOfInfluencers();
    const { startId, endId } = res
    // Calculate and assign ID ranges
    children.forEach((node, index) => {
        const idRange = ZooKeeperUtils.calculateIdRangeForNode(index, childrenCount, startId, endId);
        console.log({ idRange })
        const nodePath = `${nodesPath}/${node}`;
        updateChildrenData(zkClient, nodePath, idRange);
    });
    console.log("distributing the ids after capturing the change")
}

function listChildren(client, path) {
    client.getChildren(
        path,
        function (event) {
            console.log('Got watcher event: %s', event);
            listChildren(client, path);
        },
        function (error, children, stat) {
            if (error) {
                console.log(
                    'Failed to list children of %s due to: %s.',
                    path,
                    error
                );
                return;
            }

            console.log('Children of %s are: %j.', path, children);
            distributedIdsAmongChildren(children)

        }
    );
}

zkClient.once('connected', function () {
    console.log('Connected to ZooKeeper.');
    listChildren(zkClient, nodesPath);
});


process.on('SIGINT', () => {
    zkClient.close();
    process.exit();
});