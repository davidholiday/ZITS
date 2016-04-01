ClusterClient

Narrative:
In order to test the CRUD functions of the distributed atomic long client
As a guy who'd like to continue to get paid
I want to properlly test my code before I tell Robert it's done
			
	
Scenario: Test initizlization by token
Given a zk atomic long cluster client
When the caller initializes 524289 tokens
Then an operation requesting 1000 free tokens succeeds
And all tokens above index 524289 are set to unavailable


Scenario: Test block initialization by block
Given a zk atomic long cluster client
When the caller initializes 10 blocks
Then the correct number of empty zNodes are created


Scenario: Test double initialize failure
Given a zk atomic long cluster client
When the caller initializes 10 blocks twice
Then the second init operation fails


Scenario: Test try/set success
Given a zk atomic long cluster client
When the caller initializes 10 blocks
And the caller serializes 10 blocks of random data
Then the 10 blocks are not empty


Scenario: Test block read data integrity
Given a zk atomic long cluster client
When the caller initializes 275 blocks
And the caller serializes 275 blocks of random data
Then the 275 blocks are identical to the serialized blocks


Scenario: Test delete blocks
Given a zk atomic long cluster client
When the caller initializes 275 blocks
And the caller serializes 275 blocks of random data
And the caller deletes the blocks
Then there are no remaining blocks in zookeeper


Scenario: Test tokenID to/from block/byte/bit index array conversions
Given 1000 randomly generated tokenIDs
When the caller converts the tokenIDs to index arrays and back again
Then both sets of tokenIDs should match


Scenario: Test get available token
Given a zk atomic long cluster client
And 275 serialized blocks of random data
When the caller gets 1000 tokens
Then all the returned tokens are independently retrieved as valid


Scenario: Test set token
Given a zk atomic long cluster client
And 275 serialized blocks of random data
When the caller gets 1000 tokens
And the caller sets those tokens
Then the tokens are verified as set


Scenario: Test unset token
Given a zk atomic long cluster client
And 275 serialized blocks of random data
When the caller gets 1000 tokens that are already set
And the caller unsets those tokens
Then the tokens are verified as unset



Scenario: Test block/token count conversion
Given a zk atomic long cluster client
When the caller converts 275 blocks to token count and back
Then both block counts should be the same size






