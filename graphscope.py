import ramcloud
import RCDB_pb2 as pb
import array

class GraphScope:
  c = ramcloud.RAMCloud();
  userTableID = 0L
  tweetTableID = 0L
  idTableID = 0L

  def connect(self, coordinatorLocator):
    self.c.connect(coordinatorLocator)
    self.userTableID = self.c.get_table_id("UserTable")
    self.tweetTableID = self.c.get_table_id("TweetTable")
    self.idTableID = self.c.get_table_id("IDTable")  

  def printUserTweetIDs(self, userID):
    key = pb.Key()
    key.id = userID
    key.column = pb.Key.TWEETS
    
    readBuf = self.c.read(self.userTableID, key.SerializeToString())

    print array.array('L', readBuf[0])

  def printUserFollowerIDs(self, userID):
    key = pb.Key()
    key.id = userID
    key.column = pb.Key.FOLLOWERS
    
    readBuf = self.c.read(self.userTableID, key.SerializeToString())

    print array.array('L', readBuf[0])

  def printUserStreamIDs(self, userID):
    key = pb.Key()
    key.id = userID
    key.column = pb.Key.STREAM
    
    readBuf = self.c.read(self.userTableID, key.SerializeToString())

    print array.array('L', readBuf[0])

  def printUserStream(self, userID, pgSize):
    key = pb.Key()
    key.id = userID
    key.column = pb.Key.STREAM
    
    readBuf = self.c.read(self.userTableID, key.SerializeToString())

    tweetList = array.array('L', readBuf[0])

    for i in range(0,pgSize):
      tweetID = tweetList[len(tweetList) - 1 - i]
      print str(i+1) + ": "
      print "tweetID: " + str(tweetID)
      self.printTweet(tweetID)

  def printTweet(self, tweetID):
    key = pb.Key()
    key.id = tweetID
    key.column = pb.Key.DATA
    
    readBuf = self.c.read(self.tweetTableID, key.SerializeToString())

    tweet = pb.Tweet()
    tweet.ParseFromString(readBuf[0])

    print tweet.__str__()

def GraphScopeFactory():
  g = GraphScope()
  g.connect("infrc:host=192.168.1.156,port=12246")
  return g
