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
#include <thread>
#include <random>

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

// USERID, TXTYPE, LATENCY
#define LATFILE_HDRFMTSTR "%12s%12s%12s\n"
#define LATFILE_ENTFMTSTR "%12lu%12s%12.2f\n"

// RUNTIME, STREAM_UPDATE_FAILURES
#define DATFILE_HDRFMTSTR "%12s\n"
#define DATFILE_ENTFMTSTR "%12.2f\n"

#define NUM_STATS 10

typedef struct {
  uint64_t startTime;
  uint64_t endTime;
  uint64_t totalTime;
  uint64_t keyBytes;
  uint64_t totalKeyBytes;
  uint64_t valueBytes;
  uint64_t totalValueBytes;
  uint64_t multiOpSize;
  uint64_t totalMultiOpSize;
  uint64_t opCount;
  uint64_t rejectCount;
} opStat;

uint64_t timePassed(opStat x) {
    return x.endTime - x.startTime;
}

void
TwitterWorkloadThread(
        OptionParser& optionParser,
        uint64_t serverNumber,
        uint64_t threadNumber,
        double runTime,
        double streamProb,
        uint64_t totUsers,
        uint64_t streamTxPgSize,
        uint64_t workingSetSize,
        string outputDir) {
    LOG(NOTICE, "WorkloadThread(s%02lu,t%02lu): Starting...", serverNumber, threadNumber);

    LOG(NOTICE, "WorkloadThread(s%02lu,t%02lu): Connecting to coordinator at %s", serverNumber, threadNumber, optionParser.options.getCoordinatorLocator().c_str());
    
    // need external context to set log levels with OptionParser
    Context context(false);
    
    RamCloud client(&context,
            optionParser.options.getCoordinatorLocator().c_str(),
            optionParser.options.getClusterName().c_str());
    
    LOG(NOTICE, "WorkloadThread(s%02lu,t%02lu): Looking for userTable and tweetTable...", serverNumber, threadNumber);
    
    uint64_t userTableId = client.getTableId("UserTable");
    uint64_t tweetTableId = client.getTableId("TweetTable");
    uint64_t idTableId = client.getTableId("IDTable");
    
    LOG(NOTICE, "WorkloadThread(s%02lu,t%02lu): Found userTable (id %lu) and tweetTable (id %lu)...", serverNumber, threadNumber, userTableId, tweetTableId);
    
    string latFileName = format("%ss%02lu_t%02lu.lat", outputDir.c_str(), serverNumber, threadNumber);
    
    std::ofstream latFile;
    if(serverNumber == 0 && threadNumber == 0) {
        LOG(NOTICE, "WorkloadThread(s%02lu,t%02lu): Recording measurements in file %s", serverNumber, threadNumber, latFileName.c_str());
        latFile.open(latFileName.c_str());
        latFile << format(LATFILE_HDRFMTSTR, "#USERID", "TXTYPE", "LATENCY(us)");
    }
    
    RCDB::ProtoBuf::Key key;
//    RCDB::ProtoBuf::IDList userStream;
//    RCDB::ProtoBuf::IDList tweetStream;
//    RCDB::ProtoBuf::IDList userFollowers;
    RCDB::ProtoBuf::Tweet tweetData;
    Buffer buf;
    string keyStringBuffer;
    string valueStringBuffer;
    
    string tweetString = "The problem addressed here concerns a set of isolated processors, some unknown subset of which may be faulty, that communicate only by means";
    time_t timev;
    
    MultiReadObject requestObjects[streamTxPgSize];
    MultiReadObject* requests[streamTxPgSize];
    string tweetKeyStrings[streamTxPgSize];
    
    //Tub<ObjectBuffer> values[streamTxPgSize];
    
    // Stats tracking.
    uint64_t statLoopTimeStart, statLoopTimeEnd, statLoopTimeTotal=0;
    
    uint64_t statStTxStart, statStTxEnd, statStTxTotal=0;
    uint64_t statStTxCount = 0;
    
    uint64_t statTwTxStart, statTwTxEnd, statTwTxTotal=0;
    uint64_t statTwTxCount = 0;
        
    opStat stOpStats[NUM_STATS];
    opStat twOpStats[NUM_STATS];

    uint64_t startTime, totalTime;
    uint64_t startTime2, totalTime2;
    
    for(uint64_t i = 0; i < NUM_STATS; i++) {
        memset(&stOpStats[i], 0, sizeof(opStat));
        memset(&twOpStats[i], 0, sizeof(opStat));
    }
    
    uint64_t statStreamUpdateFailures = 0;
    
    statLoopTimeStart = Cycles::rdtsc();
    while (Cycles::toSeconds(Cycles::rdtsc() - statLoopTimeStart) < runTime * 60.0) {
        double randDouble = (double) rand() / (double) RAND_MAX;

        if (randDouble <= streamProb) {
            uint64_t userID;
            if (workingSetSize == 0) {
                userID = (rand() % totUsers) + 1;
            } else {
                userID = rand() % workingSetSize;
                userID = (userID * (totUsers / workingSetSize)) + 1;
            }

            statStTxStart = Cycles::rdtsc();
            
//            startTime = Cycles::rdtsc();
            
            key.set_id(userID);
            key.set_column(RCDB::ProtoBuf::Key::STREAM);
            keyStringBuffer = key.SerializeAsString();
            
//            totalTime = Cycles::rdtsc() - startTime;
//            printf("time0: %0.2fus\n", (double)Cycles::toNanoseconds(totalTime) / 1000.0);
            
            stOpStats[0].startTime = Cycles::rdtsc();
            client.read(userTableId, keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(), &buf);
            stOpStats[0].endTime = Cycles::rdtsc();
            stOpStats[0].totalTime += timePassed(stOpStats[0]);
            stOpStats[0].totalKeyBytes += (uint64_t) keyStringBuffer.length();
            stOpStats[0].totalValueBytes += (uint64_t) buf.size();
            stOpStats[0].opCount++;
            
//            startTime = Cycles::rdtsc();
            
            uint64_t* userStream = (uint64_t*)buf.getRange(0, buf.size());
            uint64_t userStreamLen = buf.size()/sizeof(uint64_t);
            
//            totalTime = Cycles::rdtsc() - startTime;
//            printf("time1: %0.2fus\n", (double)Cycles::toNanoseconds(totalTime) / 1000.0);
            
//            printf("WorkloadThread(s%02lu,t%02lu): Read stream for user %lu (in %luus, size %d):", serverNumber, threadNumber, userID, Cycles::toMicroseconds(statStTxRdStEnd-statStTxRdStStart), buf.size());
//            for(uint64_t i = 0; i < (uint64_t)userStreamLen; i++) 
//                printf("%8lu", userStream[i]);
//            printf("\n");
            
//            startTime = Cycles::rdtsc();
            
            uint64_t multiReadSize = std::min(userStreamLen, streamTxPgSize);
            Tub<ObjectBuffer> values[multiReadSize];
            for(uint64_t i = 0; i < multiReadSize; i++) {
//                startTime2 = Cycles::rdtsc();
                
                key.set_id(userStream[userStreamLen - 1 - i]);
                key.set_column(RCDB::ProtoBuf::Key::DATA);
                tweetKeyStrings[i] = key.SerializeAsString();
                
//                totalTime2 = Cycles::rdtsc() - startTime2;
//                printf("time2.1: %0.2fus\n", (double)Cycles::toNanoseconds(totalTime2) / 1000.0);
                
//                startTime2 = Cycles::rdtsc();
                
                requestObjects[i] =
                    MultiReadObject(tweetTableId,
                    tweetKeyStrings[i].c_str(), (uint16_t)tweetKeyStrings[i].length(), &values[i]);
                requests[i] = &requestObjects[i];
                stOpStats[1].totalKeyBytes += tweetKeyStrings[i].length();
                
//                totalTime2 = Cycles::rdtsc() - startTime2;
//                printf("time2.2: %0.2fus\n", (double)Cycles::toNanoseconds(totalTime2) / 1000.0);
                
            }
            
//            totalTime = Cycles::rdtsc() - startTime;
//            printf("time2: %0.2fus\n", (double)Cycles::toNanoseconds(totalTime) / 1000.0);
            
            // Clock the multiRead.
            stOpStats[1].startTime = Cycles::rdtsc();
            client.multiRead(requests, (uint32_t)multiReadSize);
            stOpStats[1].endTime = Cycles::rdtsc();
            stOpStats[1].totalTime += timePassed(stOpStats[1]);
            stOpStats[1].totalMultiOpSize += multiReadSize;
            stOpStats[1].opCount++;
            
            for(uint64_t i = 0; i < multiReadSize; i++) {
                uint32_t valueLen;
                const void* value = values[i].get()->getValue(&valueLen);
                
                stOpStats[1].totalValueBytes += (uint64_t)valueLen;
            }
            
//            printf("WorkloadThread(s%02lu,t%02lu): Performed stream multiread of size %lu for user %lu (in %luus) and read:\n", serverNumber, threadNumber, multiReadSize, userID, Cycles::toMicroseconds(statStTxRdTwEnd-statStTxRdTwStart));
//            for(uint64_t i = 0; i < multiReadSize; i++) {
//                uint32_t dataLen;
//                const void* data = values[i].get()->getValue(&dataLen);
//                RCDB::ProtoBuf::Tweet tweet;
//                tweet.ParseFromArray(data, dataLen);
//                key.ParseFromArray(tweetKeyStrings[i].c_str(), (int)tweetKeyStrings[i].length());
//                printf("TweetID: %9lu, dataLen: %9d, TweeterID: %9lu, Time: %9lu, Text: %s\n", key.id(), dataLen, tweet.user(), tweet.time(), tweet.text().c_str());
//            }
            
            statStTxEnd = Cycles::rdtsc();
            
            statStTxTotal += statStTxEnd - statStTxStart;
            
            statStTxCount++;
            
//            printf("Read Stream: %5lu, MultiRead Tweets: %5lu, Total Stream Tx Time: %5lu\n", 
//                    Cycles::toMicroseconds(statStTxRdStEnd - statStTxRdStStart),
//                    Cycles::toMicroseconds(statStTxRdTwEnd - statStTxRdTwStart),
//                    Cycles::toMicroseconds(statStTxEnd - statStTxStart));
            
            if(serverNumber == 0 && threadNumber == 0)
                latFile << format(LATFILE_ENTFMTSTR, userID, "ST", (double)Cycles::toNanoseconds(statStTxEnd - statStTxStart)/1000.0);
            
        } else {
            uint64_t userID;
            if (workingSetSize == 0) {
                userID = (rand() % totUsers) + 1;
            } else {
                userID = rand() % workingSetSize;
                userID = (userID * (totUsers / workingSetSize)) + 1;
            }
            
            statTwTxStart = Cycles::rdtsc();
            // First grab a unique tweetID
//            startTime = Cycles::rdtsc();
            
            RCDB::ProtoBuf::IDTableKey idTableKey;
            idTableKey.set_type(RCDB::ProtoBuf::IDTableKey::TWEETID);
            keyStringBuffer = idTableKey.SerializeAsString();
            
//            totalTime = Cycles::rdtsc() - startTime;
//            printf("time0: %0.2fus\n", (double)Cycles::toNanoseconds(totalTime) / 1000.0);
            
            twOpStats[0].startTime = Cycles::rdtsc();
            uint64_t nextTweetID = client.incrementInt64(idTableId, keyStringBuffer.c_str(), (uint16_t)keyStringBuffer.length(), 1);
            twOpStats[0].endTime = Cycles::rdtsc();
            twOpStats[0].totalTime += timePassed(twOpStats[0]);
            twOpStats[0].opCount++;
            
            // Create tweet in the tweet table.
//            startTime = Cycles::rdtsc();
            
            key.set_id(nextTweetID);
            key.set_column(RCDB::ProtoBuf::Key::DATA);
            tweetData.set_text(tweetString.substr(0, rand() % 140));
            time(&timev);
            tweetData.set_time(timev);
            tweetData.set_user(userID);
            
            keyStringBuffer = key.SerializeAsString();
            valueStringBuffer = tweetData.SerializeAsString();

//            totalTime = Cycles::rdtsc() - startTime;
//            printf("time1: %0.2fus\n", (double)Cycles::toNanoseconds(totalTime) / 1000.0);
            
            twOpStats[1].startTime = Cycles::rdtsc();
            client.write(tweetTableId,
                    keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(),
                    valueStringBuffer.c_str(), (uint32_t) valueStringBuffer.length());
            twOpStats[1].endTime = Cycles::rdtsc();
            twOpStats[1].totalTime += timePassed(twOpStats[1]);
            twOpStats[1].totalKeyBytes += (uint64_t) keyStringBuffer.length();
            twOpStats[1].totalValueBytes += (uint64_t) valueStringBuffer.length();
            twOpStats[1].opCount++;
            
            // Update the user's tweet list
//            startTime = Cycles::rdtsc();
            
            key.set_id(userID);
            key.set_column(RCDB::ProtoBuf::Key::TWEETS);
            keyStringBuffer = key.SerializeAsString();
            
//            totalTime = Cycles::rdtsc() - startTime;
//            printf("time2: %0.2fus\n", (double)Cycles::toNanoseconds(totalTime) / 1000.0);
            
            twOpStats[2].startTime = Cycles::rdtsc();
            client.read(userTableId, keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(), &buf);
            twOpStats[2].endTime = Cycles::rdtsc();
            twOpStats[2].totalTime += timePassed(twOpStats[2]);
            twOpStats[2].totalKeyBytes += (uint64_t) keyStringBuffer.length();
            twOpStats[2].totalValueBytes += (uint64_t) buf.size();
            twOpStats[2].opCount++;
            
//            startTime = Cycles::rdtsc();
            
            buf.appendCopy((const void*)&nextTweetID, sizeof(nextTweetID));
            
//            totalTime = Cycles::rdtsc() - startTime;
//            printf("time3: %0.2fus\n", (double)Cycles::toNanoseconds(totalTime) / 1000.0);
            
            twOpStats[3].startTime = Cycles::rdtsc();
            client.write(userTableId,
                    keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(),
                    buf.getRange(0, buf.size()), buf.size());
            twOpStats[3].endTime = Cycles::rdtsc();
            twOpStats[3].totalTime += timePassed(twOpStats[3]);
            twOpStats[3].totalKeyBytes += (uint64_t) keyStringBuffer.length();
            twOpStats[3].totalValueBytes += (uint64_t) buf.size();
            twOpStats[3].opCount++;
            
            // Update the user's followers
//            startTime = Cycles::rdtsc();
            
            key.set_id(userID);
            key.set_column(RCDB::ProtoBuf::Key::FOLLOWERS);
            keyStringBuffer = key.SerializeAsString();
            
//            totalTime = Cycles::rdtsc() - startTime;
//            printf("time4: %0.2fus\n", (double)Cycles::toNanoseconds(totalTime) / 1000.0);
            
            twOpStats[4].startTime = Cycles::rdtsc();
            client.read(userTableId, keyStringBuffer.c_str(), (uint16_t) keyStringBuffer.length(), &buf);
            twOpStats[4].endTime = Cycles::rdtsc();
            twOpStats[4].totalTime += timePassed(twOpStats[4]);
            twOpStats[4].totalKeyBytes += (uint64_t) keyStringBuffer.length();
            twOpStats[4].totalValueBytes += (uint64_t) buf.size();
            twOpStats[4].opCount++;
            
//            startTime = Cycles::rdtsc();
            
            uint64_t* userFollowers = (uint64_t*)buf.getRange(0, buf.size());
            uint64_t numFollowers = buf.size()/sizeof(uint64_t);
            
            MultiReadObject readRequestObjects[numFollowers];
            MultiReadObject* readRequests[numFollowers];
            MultiWriteObject writeRequestObjects[numFollowers];
            MultiWriteObject* writeRequests[numFollowers];
            RejectRules rejectRules[numFollowers];
            RCDB::ProtoBuf::Key userStreamKeys[numFollowers];
//            RCDB::ProtoBuf::IDList userStreamValues[numFollowers];
            string userStreamKeyStrings[numFollowers];
            string userStreamValueStrings[numFollowers];
            Tub<ObjectBuffer> values[numFollowers];
            Buffer valueBufs[numFollowers];
            for(uint64_t i = 0; i < numFollowers; i++) {
                userStreamKeys[i].set_id(userFollowers[i]);
                userStreamKeys[i].set_column(RCDB::ProtoBuf::Key::STREAM);
                userStreamKeyStrings[i] = userStreamKeys[i].SerializeAsString();
                readRequestObjects[i] =
                        MultiReadObject(userTableId,
                        userStreamKeyStrings[i].c_str(), (uint16_t) userStreamKeyStrings[i].length(), &values[i]);
                readRequests[i] = &readRequestObjects[i];
                twOpStats[5].totalKeyBytes += (uint64_t) userStreamKeyStrings[i].length();
            }
            
//            totalTime = Cycles::rdtsc() - startTime;
//            printf("time5: %0.2fus\n", (double)Cycles::toNanoseconds(totalTime) / 1000.0);
            
            twOpStats[5].startTime = Cycles::rdtsc();
            client.multiRead(readRequests, (uint32_t) numFollowers);
            twOpStats[5].endTime = Cycles::rdtsc();
            twOpStats[5].totalTime += timePassed(twOpStats[5]);
            twOpStats[5].totalMultiOpSize += numFollowers;
            twOpStats[5].opCount++;
            
//            startTime = Cycles::rdtsc();
            
            for(uint64_t i = 0; i < numFollowers; i++) {
//                startTime2 = Cycles::rdtsc();
                
                // Trigger initialization of internal Tub<Object>
                uint32_t valueLen;
                const void* value = values[i].get()->getValue(&valueLen);
                
                twOpStats[5].totalValueBytes += (uint64_t)valueLen;
                
                // Create Buffer to store ObjectBuffer value and tack on new Tweet ID
                valueBufs[i].appendExternal(value, valueLen);
                valueBufs[i].appendCopy((const void*)&nextTweetID, sizeof(nextTweetID));
                
//                totalTime2 = Cycles::rdtsc() - startTime2;
//                printf("time6.1: %0.2fus\n", (double)Cycles::toNanoseconds(totalTime2) / 1000.0);
                
//                startTime2 = Cycles::rdtsc();
                
                memset(&rejectRules[i], 0, sizeof(RejectRules));
                rejectRules[i].givenVersion = values[i].get()->object.get()->getVersion();
                rejectRules[i].versionLeGiven = 1;
                
//                totalTime2 = Cycles::rdtsc() - startTime2;
//                printf("time6.3: %0.2fus\n", (double)Cycles::toNanoseconds(totalTime2) / 1000.0);
                
//                startTime2 = Cycles::rdtsc();
                
                writeRequestObjects[i] = 
                        MultiWriteObject(userTableId,
                        userStreamKeyStrings[i].c_str(), (uint16_t) userStreamKeyStrings[i].length(),
                        valueBufs[i].getRange(0, valueBufs[i].size()), valueBufs[i].size(),
                        &rejectRules[i]);
                writeRequests[i] = &writeRequestObjects[i];
                twOpStats[6].totalKeyBytes += (uint64_t) userStreamKeyStrings[i].length();
                twOpStats[6].totalValueBytes += (uint64_t) valueBufs[i].size();
                
//                totalTime2 = Cycles::rdtsc() - startTime2;
//                printf("time6.4: %0.2fus\n", (double)Cycles::toNanoseconds(totalTime2) / 1000.0);
            }
            
//            totalTime = Cycles::rdtsc() - startTime;
//            printf("time6: %0.2fus\n", (double)Cycles::toNanoseconds(totalTime) / 1000.0);
            
            twOpStats[6].startTime = Cycles::rdtsc();
            client.multiWrite(writeRequests, (uint32_t) numFollowers);
            twOpStats[6].endTime = Cycles::rdtsc();
            twOpStats[6].totalTime += timePassed(twOpStats[6]);
            twOpStats[6].totalMultiOpSize += numFollowers;
            twOpStats[6].opCount++;
            
            for(uint64_t i = 0; i < numFollowers; i++)
                if(writeRequests[i]->status != Status::STATUS_OK)
                    twOpStats[6].rejectCount++;
            
            statTwTxEnd = Cycles::rdtsc();
            statTwTxTotal += statTwTxEnd - statTwTxStart;
            
            statTwTxCount++;
            
            if(serverNumber == 0 && threadNumber == 0)
                latFile << format(LATFILE_ENTFMTSTR, userID, "TW", (double)Cycles::toNanoseconds(statTwTxEnd - statTwTxStart)/1000.0);
        }
    }
    statLoopTimeEnd = Cycles::rdtsc();
    
    statLoopTimeTotal = statLoopTimeEnd - statLoopTimeStart;

    if(serverNumber == 0 && threadNumber == 0)
        latFile.close();

    string datFileName = format("%ss%02lu_t%02lu.dat", outputDir.c_str(), serverNumber, threadNumber);
    LOG(NOTICE, "WorkloadThread(s%02lu,t%02lu): Recording summary information in file %s", serverNumber, threadNumber, datFileName.c_str());
    std::ofstream datFile(datFileName.c_str());
    
    datFile << format("%-35s:%0.2fs\n", "RUNTIME", Cycles::toSeconds(statLoopTimeTotal));
    datFile << format("%-35s:%lu\n", "STREAM UPDATE FAILURES", statStreamUpdateFailures);
    datFile << format("%-35s:%lu\n", "STREAM TRANSACTIONS", statStTxCount);
    datFile << format("%-35s:%0.2fus\n", "AVERAGE STREAM TX TIME", (double)Cycles::toNanoseconds(statStTxTotal) / (double)statStTxCount / 1000.0);
    datFile << format("%-35s:%0.2fus (Key: %0.2fB, Value: %0.2fB)\n", "AVERAGE READ USERID:STREAM", (double)Cycles::toNanoseconds(stOpStats[0].totalTime) / (double)stOpStats[0].opCount / 1000.0, (double)stOpStats[0].totalKeyBytes / (double)stOpStats[0].opCount, (double)stOpStats[0].totalValueBytes / (double)stOpStats[0].opCount);
    datFile << format("%-35s:%0.2fus (Key: %0.2fB, Value: %0.2fB, MOpSize: %0.2f)\n", "AVERAGE MULTIREAD TWEET:DATA", (double)Cycles::toNanoseconds(stOpStats[1].totalTime) / (double)stOpStats[1].opCount / 1000.0, (double)stOpStats[1].totalKeyBytes / (double)stOpStats[1].totalMultiOpSize, (double)stOpStats[1].totalValueBytes / (double)stOpStats[1].totalMultiOpSize, (double)stOpStats[1].totalMultiOpSize / (double)stOpStats[1].opCount);
    datFile << format("%-35s:%lu\n", "TWEET TRANSACTIONS", statTwTxCount);
    datFile << format("%-35s:%0.2fus\n", "AVERAGE TWEET TX TIME", (double)Cycles::toNanoseconds(statTwTxTotal) / (double)statTwTxCount / 1000.0);
    datFile << format("%-35s:%0.2fus\n", "AVERAGE INCREMENT TWEETID", (double)Cycles::toNanoseconds(twOpStats[0].totalTime) / (double)twOpStats[0].opCount / 1000.0);
    datFile << format("%-35s:%0.2fus (Key: %0.2fB, Value: %0.2fB)\n", "AVERAGE WRITE TWEETID:DATA", (double)Cycles::toNanoseconds(twOpStats[1].totalTime) / (double)twOpStats[1].opCount / 1000.0, (double)twOpStats[1].totalKeyBytes / (double)twOpStats[1].opCount, (double)twOpStats[1].totalValueBytes / (double)twOpStats[1].opCount);
    datFile << format("%-35s:%0.2fus (Key: %0.2fB, Value: %0.2fB)\n", "AVERAGE READ USERID:TWEETS", (double)Cycles::toNanoseconds(twOpStats[2].totalTime) / (double)twOpStats[2].opCount / 1000.0, (double)twOpStats[2].totalKeyBytes / (double)twOpStats[2].opCount, (double)twOpStats[2].totalValueBytes / (double)twOpStats[2].opCount);
    datFile << format("%-35s:%0.2fus (Key: %0.2fB, Value: %0.2fB)\n", "AVERAGE WRITE USERID:TWEETS", (double)Cycles::toNanoseconds(twOpStats[3].totalTime) / (double)twOpStats[3].opCount / 1000.0, (double)twOpStats[3].totalKeyBytes / (double)twOpStats[3].opCount, (double)twOpStats[3].totalValueBytes / (double)twOpStats[3].opCount);
    datFile << format("%-35s:%0.2fus (Key: %0.2fB, Value: %0.2fB)\n", "AVERAGE READ USERID:FOLLOWERS", (double)Cycles::toNanoseconds(twOpStats[4].totalTime) / (double)twOpStats[4].opCount / 1000.0, (double)twOpStats[4].totalKeyBytes / (double)twOpStats[4].opCount, (double)twOpStats[4].totalValueBytes / (double)twOpStats[4].opCount);
    datFile << format("%-35s:%0.2fus (Key: %0.2fB, Value: %0.2fB, MOpSize: %0.2f)\n", "AVERAGE MULTIREAD USERID:STREAM", (double)Cycles::toNanoseconds(twOpStats[5].totalTime) / (double)twOpStats[5].opCount / 1000.0, (double)twOpStats[5].totalKeyBytes / (double)twOpStats[5].totalMultiOpSize, (double)twOpStats[5].totalValueBytes / (double)twOpStats[5].totalMultiOpSize, (double)twOpStats[5].totalMultiOpSize / (double)twOpStats[5].opCount);
    datFile << format("%-35s:%0.2fus (Key: %0.2fB, Value: %0.2fB, MOpSize: %0.2f, RejectCount: %lu)\n", "AVERAGE MULTIWRITE USERID:STREAM", (double)Cycles::toNanoseconds(twOpStats[6].totalTime) / (double)twOpStats[6].opCount / 1000.0, (double)twOpStats[6].totalKeyBytes / (double)twOpStats[6].totalMultiOpSize, (double)twOpStats[6].totalValueBytes / (double)twOpStats[6].totalMultiOpSize, (double)twOpStats[6].totalMultiOpSize / (double)twOpStats[6].opCount, twOpStats[6].rejectCount);
}

int
main(int argc, char *argv[])
try {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    uint64_t clientIndex;
    uint64_t numClients;
    uint64_t numThreads;
    double runTime;
    double streamProb;
    uint64_t totUsers;
    uint64_t streamTxPgSize;
    uint64_t workingSetSize;
    string outputDir;

    // Set line buffering for stdout so that printf's and log messages
    // interleave properly.
    setvbuf(stdout, NULL, _IOLBF, 1024);

    // need external context to set log levels with OptionParser
    Context context(false);

    OptionsDescription clientOptions("TwitterWorkloadClient");
    clientOptions.add_options()
            ("clientIndex",
            ProgramOptions::value<uint64_t>(&clientIndex)->
                default_value(0),
            "Index of this client (first client is 0; default 0)")
            ("numClients",
            ProgramOptions::value<uint64_t>(&numClients)->
                default_value(1),
            "Total number of client servers running (default 1)")    
            ("numThreads",
            ProgramOptions::value<uint64_t>(&numThreads)->
                default_value(1),
            "Total number of threads spread over clients (default 1).")
            ("runTime",
            ProgramOptions::value<double>(&runTime)->
                default_value(0.1),
            "Total time to run (minutes; default 0.1).")
            ("streamProb",
            ProgramOptions::value<double>(&streamProb)->
                default_value(0.9),
            "Probability of performing a stream operation (default 0.9).")
            ("totUsers",
            ProgramOptions::value<uint64_t>(&totUsers),
            "Total number of users (required).")
            ("streamTxPgSize",
            ProgramOptions::value<uint64_t>(&streamTxPgSize)->
                default_value(8),
            "Number of tweets to fetch in a stream operation (default 8).")
            ("workingSetSize",
            ProgramOptions::value<uint64_t>(&workingSetSize)->
                default_value(0),
            "Number of users over which to apply workload (0 for all users; default 0).")
            ("outputDir",
            ProgramOptions::value<string>(&outputDir)->
                default_value("./"),
            "Output directory for measurement files (default \"./\".");


    OptionParser optionParser(clientOptions, argc, argv);

    LOG(NOTICE, "TwitterWorkloadClient: \n"
            "clientIndex: %lu\n"
            "numClients: %lu\n"
            "numThreads: %lu\n"
            "runTime: %0.2f\n"
            "streamProb: %0.2f\n"
            "totUsers: %lu\n"
            "streamTxPgSize: %lu\n"
            "workingSetSize: %lu\n"
            "outputDir: %s\n",
            clientIndex,
            numClients,
            numThreads,
            runTime,
            streamProb,
            totUsers,
            streamTxPgSize,
            workingSetSize,
            outputDir.c_str());

    uint64_t numLocalThreads = numThreads / numClients;
    numLocalThreads += ((numThreads % numClients) > clientIndex) ? 1 : 0;

    LOG(NOTICE, "Launching workload threads...");

    Tub<std::thread> threads[numLocalThreads];

    for (uint64_t i = 0; i < numLocalThreads; i++)
        threads[i].construct(TwitterWorkloadThread, optionParser, clientIndex, i, runTime, streamProb, totUsers, streamTxPgSize, workingSetSize, outputDir);

    for (uint64_t i = 0; i < numLocalThreads; i++)
        threads[i].get()->join();

    return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
