/* Copyright (c) 2009-2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <assert.h>
#include <fstream>

#include "ClusterMetrics.h"
#include "Cycles.h"
#include "ShortMacros.h"
#include "Crc32C.h"
#include "ObjectFinder.h"
#include "OptionParser.h"
#include "RamCloud.h"
#include "Tub.h"

#include "RCDB.pb.h"

using namespace RAMCloud;

/*
 * Speed up recovery insertion with the single-shot FillWithTestData RPC.
 */
bool fillWithTestData = false;

int
main(int argc, char *argv[])
try {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    uint64_t clientIndex;
    uint64_t numClients;
    
    uint64_t totalUsers;
    uint64_t tweetsPerUser;
    string edgeListFileName;

    uint64_t STARTING_TWEET_TIME = 1230800000;
    uint64_t TWEETS_PER_SECOND = 1000;

    // Set line buffering for stdout so that printf's and log messages
    // interleave properly.
    setvbuf(stdout, NULL, _IOLBF, 1024);

    // need external context to set log levels with OptionParser
    Context context(false);

    OptionsDescription clientOptions("TwitterGraphBatchLoader");
    clientOptions.add_options()
            // These first two arguments are currently ignored. They are here
            // so that this client can be run with cluster.py
            ("clientIndex",
            ProgramOptions::value<uint64_t>(&clientIndex)->
                default_value(0),
            "Index of this client (first client is 0; currently ignored)")
            ("numClients",
            ProgramOptions::value<uint64_t>(&numClients)->
                default_value(1),
            "Total number of clients running (currently ignored)")
    
            ("totalUsers",
            ProgramOptions::value<uint64_t>(&totalUsers),
            "Total number of users.")
            ("tweetsPerUser",
            ProgramOptions::value<uint64_t>(&tweetsPerUser)->
            default_value(1),
            "Number of tweets to seed each user with.")
            ("edgeList",
            ProgramOptions::value<string>(&edgeListFileName),
            "Edgelist file to load into RAMCloud");

    OptionParser optionParser(clientOptions, argc, argv);

    LOG(NOTICE, "TwitterGraphBatchLoader: totalUsers: %lu, tweetsPerUser: %lu, edgeList: %s", totalUsers, tweetsPerUser, edgeListFileName.c_str());
    
    context.transportManager->setSessionTimeout(
            optionParser.options.getSessionTimeout());

    LOG(NOTICE, "connecting to %s with cluster name %s",
            optionParser.options.getCoordinatorLocator().c_str(),
            optionParser.options.getClusterName().c_str());

    RamCloud client(&context,
            optionParser.options.getCoordinatorLocator().c_str(),
            optionParser.options.getClusterName().c_str());

    uint64_t userTableId = client.createTable("UserTable");
    uint64_t tweetTableId = client.createTable("TweetTable");
    uint64_t idTableId = client.createTable("IDTable");

    LOG(NOTICE, "created/found userTable (id %lu), tweetTable (id %lu), and IDTable (id %lu)\n", userTableId, tweetTableId, idTableId);

    std::ifstream edgeListFileStream(edgeListFileName.c_str());

    string tweetString = "The problem addressed here concerns a set of isolated processors, some unknown subset of which may be faulty, that communicate only by means";

    int64_t srcID, dstID;
    int64_t curSrcID = -1;
    std::vector<uint64_t> userFollowers;
    std::vector<uint64_t> userStream;
    std::vector<uint64_t> userTweets;
    RCDB::ProtoBuf::Tweet tweetData;
    RCDB::ProtoBuf::Key key;
    uint64_t lineCount = 0;
    uint64_t writeCount = 0;
    uint64_t keyByteCount = 0;
    uint64_t valueByteCount = 0;
    uint64_t start_time = Cycles::rdtsc();
    while (edgeListFileStream >> srcID >> dstID) {
        if (curSrcID == -1)
            curSrcID = srcID;

        if (curSrcID == srcID) {
            //LOG(NOTICE, "Adding Follower %lu", dstID);
            userFollowers.push_back(dstID);
        } else {
            // Write USERID:FOLLOWERS for this user.
            key.set_id(curSrcID);
            key.set_column(RCDB::ProtoBuf::Key::FOLLOWERS);

            string keyStringBuffer = key.SerializeAsString();

            //LOG(NOTICE, "Writing USERID:FOLLOWERS with size %d", (int) (keyStringBuffer.length() + valueStringBuffer.length()));
            client.write(userTableId,
                    keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(),
                    (const void*)userFollowers.data(), (uint32_t)userFollowers.size()*(uint32_t)sizeof(uint64_t));

            writeCount++;

            keyByteCount += keyStringBuffer.length();
            valueByteCount += userFollowers.size()*sizeof(uint64_t);

            key.Clear();

            // Write USERID:STREAM for this user.
            key.set_id(curSrcID);
            key.set_column(RCDB::ProtoBuf::Key::STREAM);

            for (uint64_t tweetNumber = 0; tweetNumber < tweetsPerUser; tweetNumber++)
                for (uint64_t friendNumber = 0; friendNumber < (uint64_t) userFollowers.size(); friendNumber++)
                    userStream.push_back((totalUsers * tweetNumber) + userFollowers[friendNumber]);

            keyStringBuffer = key.SerializeAsString();

            //LOG(NOTICE, "Writing USERID:STREAM with size %d", (int) (keyStringBuffer.length() + valueStringBuffer.length()));
            client.write(userTableId,
                    keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(),
                    (const void*)userStream.data(), (uint32_t)userStream.size()*(uint32_t)sizeof(uint64_t));

            writeCount++;

            keyByteCount += keyStringBuffer.length();
            valueByteCount += userStream.size()*sizeof(uint64_t);

            key.Clear();
            userStream.clear();
            userFollowers.clear();

            // Write TWEETID:DATA for each tweet from this user.
            for (uint64_t i = 0; i < tweetsPerUser; i++) {
                key.set_id((totalUsers * i) + curSrcID);
                key.set_column(RCDB::ProtoBuf::Key::DATA);
                tweetData.set_text(tweetString.substr(0, rand() % 140));
                tweetData.set_time(STARTING_TWEET_TIME + ((totalUsers * i) + curSrcID) / TWEETS_PER_SECOND);
                tweetData.set_user(curSrcID);

                string keyStringBuffer = key.SerializeAsString();
                string valueStringBuffer = tweetData.SerializeAsString();

                //LOG(NOTICE, "Writing TWEET:DATA with size %d", (int) (keyStringBuffer.length() + valueStringBuffer.length()));
                client.write(tweetTableId,
                        keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(),
                        valueStringBuffer.c_str(), (uint32_t) valueStringBuffer.length());

                writeCount++;

                keyByteCount += keyStringBuffer.length();
                valueByteCount += valueStringBuffer.length();

                key.Clear();
                tweetData.Clear();
            }

            // Write USERID:TWEETS for this user.
            key.set_id(curSrcID);
            key.set_column(RCDB::ProtoBuf::Key::TWEETS);

            for (uint64_t i = 0; i < tweetsPerUser; i++)
                userTweets.push_back((totalUsers * i) + curSrcID);

            keyStringBuffer = key.SerializeAsString();

            //LOG(NOTICE, "Writing USERID:TWEETS with size %d", (int) (keyStringBuffer.length() + valueStringBuffer.length()));
            client.write(userTableId,
                    keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(),
                    (const void*)userTweets.data(), (uint32_t)userTweets.size()*(uint32_t)sizeof(uint64_t));
            
            writeCount++;

            keyByteCount += keyStringBuffer.length();
            valueByteCount += userTweets.size()*sizeof(uint64_t);

            key.Clear();
            userTweets.clear();

            curSrcID = srcID;

            userFollowers.push_back(dstID);
        }

        lineCount++;

        if (lineCount % 100000 == 0)
            LOG(NOTICE, "processed %lu edges (%0.2f MB to RamCloud) in %0.2f seconds, avg. %0.2f MB/s", lineCount, (float) (keyByteCount + valueByteCount) / 1000000.0, (float) Cycles::toSeconds(Cycles::rdtsc() - start_time), ((float) (keyByteCount + valueByteCount) / (float) Cycles::toSeconds(Cycles::rdtsc() - start_time)) / 1000000.0);
    }

    // Write USERID:FOLLOWERS for this user.
    key.set_id(curSrcID);
    key.set_column(RCDB::ProtoBuf::Key::FOLLOWERS);

    string keyStringBuffer = key.SerializeAsString();

    //LOG(NOTICE, "Writing USERID:FOLLOWERS with size %d", (int) (keyStringBuffer.length() + valueStringBuffer.length()));
    client.write(userTableId,
            keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(),
            (const void*)userFollowers.data(), (uint32_t)userFollowers.size()*(uint32_t)sizeof(uint64_t));

    writeCount++;

    keyByteCount += keyStringBuffer.length();
    valueByteCount += userFollowers.size()*sizeof(uint64_t);

    key.Clear();

    // Write USERID:STREAM for this user.
    key.set_id(curSrcID);
    key.set_column(RCDB::ProtoBuf::Key::STREAM);

    for (uint64_t tweetNumber = 0; tweetNumber < tweetsPerUser; tweetNumber++)
        for (uint64_t friendNumber = 0; friendNumber < (uint64_t) userFollowers.size(); friendNumber++)
            userStream.push_back((totalUsers * tweetNumber) + userFollowers[friendNumber]);

    keyStringBuffer = key.SerializeAsString();

    //LOG(NOTICE, "Writing USERID:STREAM with size %d", (int) (keyStringBuffer.length() + valueStringBuffer.length()));
    client.write(userTableId,
            keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(),
            (const void*)userStream.data(), (uint32_t)userStream.size()*(uint32_t)sizeof(uint64_t));

    writeCount++;

    keyByteCount += keyStringBuffer.length();
    valueByteCount += userStream.size()*sizeof(uint64_t);

    key.Clear();
    userStream.clear();
    userFollowers.clear();

    // Write TWEETID:DATA for each tweet from this user.
    for (uint64_t i = 0; i < tweetsPerUser; i++) {
        key.set_id((totalUsers * i) + curSrcID);
        key.set_column(RCDB::ProtoBuf::Key::DATA);
        tweetData.set_text(tweetString.substr(0, rand() % 140));
        tweetData.set_time(STARTING_TWEET_TIME + ((totalUsers * i) + curSrcID) / TWEETS_PER_SECOND);
        tweetData.set_user(curSrcID);

        string keyStringBuffer = key.SerializeAsString();
        string valueStringBuffer = tweetData.SerializeAsString();

        //LOG(NOTICE, "Writing TWEET:DATA with size %d", (int) (keyStringBuffer.length() + valueStringBuffer.length()));
        client.write(tweetTableId,
                keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(),
                valueStringBuffer.c_str(), (uint32_t) valueStringBuffer.length());

        writeCount++;

        keyByteCount += keyStringBuffer.length();
        valueByteCount += valueStringBuffer.length();

        key.Clear();
        tweetData.Clear();
    }

    // Write USERID:TWEETS for this user.
    key.set_id(curSrcID);
    key.set_column(RCDB::ProtoBuf::Key::TWEETS);

    for (uint64_t i = 0; i < tweetsPerUser; i++)
        userTweets.push_back((totalUsers * i) + curSrcID);

    keyStringBuffer = key.SerializeAsString();

    //LOG(NOTICE, "Writing USERID:TWEETS with size %d", (int) (keyStringBuffer.length() + valueStringBuffer.length()));
    client.write(userTableId,
            keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(),
            (const void*)userTweets.data(), (uint32_t)userTweets.size()*(uint32_t)sizeof(uint64_t));

    writeCount++;

    keyByteCount += keyStringBuffer.length();
    valueByteCount += userTweets.size()*sizeof(uint64_t);

    key.Clear();
    userTweets.clear();

    // Finally create userID and tweetID generators in idTable.
    RCDB::ProtoBuf::IDTableKey idTableKey;
    idTableKey.set_type(RCDB::ProtoBuf::IDTableKey::USERID);
    keyStringBuffer = idTableKey.SerializeAsString();
    
    client.write(idTableId, 
            keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(),
            (const void*)&curSrcID, sizeof(uint64_t));
    
    //printf("nextUserID: %lu\n", client.increment(idTableId, keyStringBuffer.c_str(), (uint16_t)keyStringBuffer.length(), 1));
    
    idTableKey.set_type(RCDB::ProtoBuf::IDTableKey::TWEETID);
    keyStringBuffer = idTableKey.SerializeAsString();
    uint64_t curMaxTweetID = (totalUsers * (tweetsPerUser-1)) + curSrcID;
    
    client.write(idTableId,
            keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(),
            (const void*)&curMaxTweetID, sizeof(uint64_t));
    
    //printf("nextTweetID: %lu\n", client.increment(idTableId, keyStringBuffer.c_str(), (uint16_t)keyStringBuffer.length(), 1));
    
    // Check out user.
//    uint64_t readUserID = 99999;
//    key.set_id(readUserID);
//    key.set_column(RCDB::ProtoBuf::Key::TWEETS);
//    Buffer buf;
//
//    keyStringBuffer = key.SerializeAsString();
//    client.read(userTableId, keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(), &buf);
//    userTweets.ParseFromArray(buf.getRange(0, buf.size()), buf.size());
//    LOG(NOTICE, "i: %lu, Tweets: %s", readUserID, userTweets.DebugString().c_str());
//
//    for (int i = 0; i < userTweets.id_size(); i++) {
//        key.set_id(userTweets.id(i));
//        key.set_column(RCDB::ProtoBuf::Key::DATA);
//        keyStringBuffer = key.SerializeAsString();
//        client.read(tweetTableId, keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(), &buf);
//        tweetData.ParseFromArray(buf.getRange(0, buf.size()), buf.size());
//        LOG(NOTICE, "i: %lu, TweetId: %lu, Tweet Content: %s", readUserID, userTweets.id(i), tweetData.DebugString().c_str());
//    }
//    
//    key.set_id(readUserID);
//    key.set_column(RCDB::ProtoBuf::Key::FOLLOWERS);
//    keyStringBuffer = key.SerializeAsString();
//    client.read(userTableId, keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(), &buf);
//    userFollowers.ParseFromArray(buf.getRange(0, buf.size()), buf.size());
//
//    LOG(NOTICE, "i: %lu, Followers: %s", readUserID, userFollowers.DebugString().c_str());
//    
//    key.set_id(readUserID);
//    key.set_column(RCDB::ProtoBuf::Key::STREAM);
//    keyStringBuffer = key.SerializeAsString();
//    client.read(userTableId, keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(), &buf);
//    userStream.ParseFromArray(buf.getRange(0, buf.size()), buf.size());
//
//    LOG(NOTICE, "i: %lu, Stream: %s", readUserID, userStream.DebugString().c_str());
//
//    for (int i = 0; i < userStream.id_size(); i++) {
//        key.set_id(userStream.id(i));
//        key.set_column(RCDB::ProtoBuf::Key::DATA);
//        keyStringBuffer = key.SerializeAsString();
//        client.read(tweetTableId, keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(), &buf);
//        tweetData.ParseFromArray(buf.getRange(0, buf.size()), buf.size());
//        LOG(NOTICE, "i: %lu, TweetId: %lu, Tweet Content: %s", readUserID, userStream.id(i), tweetData.DebugString().c_str());
//    }
    
    return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
